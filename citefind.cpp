#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <sys/stat.h>
#include <MySQL.hpp>
#include <json.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tempfile.hpp>

using std::cerr;
using std::chrono::seconds;
using std::cout;
using std::endl;
using std::find;
using std::ifstream;
using std::regex;
using std::regex_search;
using std::stoi;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using std::to_string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::sql_ready;
using strutils::substitute;

string myerror = "";
string mywarning = "";

struct Args {
  Args() : center(), clean_tmpdir(true) { }

  string center;
  bool clean_tmpdir;
} g_args;

struct ConfigData {
  struct Center {
    Center() : id(), db_data(), api_data() { }

    struct DB_Data {
      DB_Data() : host(), username(), password(), doi_query(), insert_table()
      { }

      string host, username, password;
      string doi_query, insert_table;
    };

    struct API_Data {
      API_Data() : url(), doi_response() { }

      string url, doi_response;
    };

    string id;
    DB_Data db_data;
    API_Data api_data;
  };

  ConfigData() : tmpdir(), centers() { }

  string tmpdir;
  vector<Center> centers;
} g_config_data;

stringstream g_myoutput;
stringstream g_mail_message;
unordered_map<string, string> g_journal_abbreviations, g_publisher_fixups;
unordered_set<string> g_journals_no_abbreviation;
const regex email_re("(.)*@(.)*\\.(.)*");

void clean_up() {
  if (g_args.clean_tmpdir) {
    stringstream oss, ess;
    unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + g_config_data.tmpdir + "/*.json\"",
        oss, ess);
  }
  if (!myerror.empty()) {
    g_mail_message << myerror << endl;
  }
  if (!g_myoutput.str().empty()) {
    g_mail_message << g_myoutput.str() << endl;
  }
  if (!g_mail_message.str().empty()) {
    unixutils::sendmail("dattore@ucar.edu", "dattore@ucar.edu", "", "citefind "
        "cron", g_mail_message.str());
  }
}

void add_to_error_and_exit(string msg) {
  append(myerror, msg, "\n");
  exit(1);
}

string journal_abbreviation(string journal_name) {
  auto parts=split(journal_name);
  if (parts.size() == 1) {
    return journal_name;
  }
  string abbreviation;
  size_t count=0,last_count=0;
  for (auto& part : parts) {
    if (part.back() == ',' || part.back() == ':' || part.back() == '.') {
      part.pop_back();
    }
    if (last_count > 0) {
      abbreviation+=" ";
    }
    if (g_journal_abbreviations.find(part) != g_journal_abbreviations.end()) {
      abbreviation+=g_journal_abbreviations[part];
      last_count=g_journal_abbreviations[part].length();
      count+=last_count;
    } else {
      abbreviation+=part;
      last_count=part.length();
    }
  }
  if (count > 0) {
    return abbreviation;
  }
  return journal_name;
}

bool inserted_book_data(string isbn, MySQL::Server& server) {
  auto fn_isbn = g_config_data.tmpdir + "/cache/" + isbn;
  struct stat buf;
  if (stat((fn_isbn + ".google.json").c_str(),&buf) != 0) {
    sleep_for(seconds(1));
    stringstream oss, ess;
    unixutils::mysystem2("/bin/tcsh -c \"curl -s -o " + fn_isbn +
        ".google.json 'https://www.googleapis.com/books/v1/volumes?q=isbn:" +
        isbn + "'\"", oss, ess);
    if (!ess.str().empty()) {
      myerror += "\nError retrieving book data from Google";
      return false;
    }
  }
  ifstream ifs((fn_isbn+".google.json").c_str());
  JSON::Object isbn_obj(ifs);
  if (!isbn_obj) {
    myerror+="\nError reading JSON : '"+myerror+"'";
    return false;
  }
  for (size_t m = 0; m < isbn_obj["items"][0]["volumeInfo"]["authors"].size();
      ++m) {
    auto sp = split(isbn_obj["items"][0]["volumeInfo"]["authors"][m].
        to_string());
    auto fnam = sp.front();
    string mnam;
    size_t next = 1;
    if (sp.size() > 2) {
      mnam = sp[next++];
    }
    string lnam;
    for (size_t n = next; n < sp.size(); ++n) {
      if (!lnam.empty()) {
        lnam += " ";
      }
      lnam += sp[n];
    }
    if (server.insert("citation.works_authors", ("ID, ID_type, last_name, "
        "first_name, middle_name, sequence"), ("'" + isbn + "', 'ISBN', '" +
        sql_ready(lnam) + "', '" + sql_ready(fnam) + "', '" + sql_ready(mnam) +
        "', " + to_string(m)), "update ID_type = values(ID_type), sequence = "
        "values(sequence)") < 0) {
      myerror += "\nError while inserting author (" + isbn + ",ISBN," + lnam +
          "," + fnam + "," + mnam + "): '" + server.error() + "'";
      return false;
    }
  }
  auto ttl = sql_ready(substitute(isbn_obj["items"][0]["volumeInfo"]["title"].
      to_string(),"\\","\\\\"));
  if (server.insert("citation.book_works", "ISBN, title, publisher", "'" + isbn
      + "', '" + ttl + "', '" + isbn_obj["items"][0]["volumeInfo"]["publisher"].
      to_string() + "'", "update title = values(title), publisher = values("
      "publisher)") < 0) {
    myerror += "\nError while inserting book data (" + isbn + "," + ttl + "," +
        isbn_obj["items"][0]["volumeInfo"]["publisher"].to_string() + "): '" +
        server.error() + "'";
    return false;
  }
  return true;
}

void query_crossref(vector<string>& doi_list, MySQL::Server& server) {
  for (const auto& doi : doi_list) {
    auto filename = doi;
    replace_all(filename, "/", "@@");
    filename = g_config_data.tmpdir + "/" + filename + ".crossref.json";
    auto ntry = 0;
    string try_error;
    while (ntry < 3) {
      struct stat buf;
      if (stat(filename.c_str(), &buf) != 0) {
        sleep_for(seconds(3));
        stringstream oss, ess;
        unixutils::mysystem2("/bin/tcsh -c \"curl -s -o " + filename +
            " 'https://api.eventdata.crossref.org/v1/events?source=crossref&"
            "obj-id=" + doi + "'\"", oss, ess);
        if (!ess.str().empty()) {
          myerror += "\nError while getting CrossRef citations for DOI '" + doi
              + "': '" + ess.str() + "'";
          continue;
        }
      }
      ifstream ifs(filename.c_str());
      JSON::Object doi_obj;
      try {
        doi_obj.fill(ifs);
      } catch (...) {
        myerror += "\nunable to create JSON object from file '" + filename +
            "'";
        continue;
      }
      ifs.close();
      if (doi_obj) {
        if (doi_obj["status"].to_string() == "ok") {
          for (size_t n = 0; n < doi_obj["message"]["events"].size(); ++n) {
            auto sid = doi_obj["message"]["events"][n]["subj_id"].to_string();
            replace_all(sid, "\\/", "/");
            auto sp = split(sid, "doi.org/");
            auto sdoi = sp.back();
            if (server.insert("citation.data_citations", ("DOI_data, DOI_work,"
                "new_flag"), ("'" + doi + "', '" + sdoi + "', '1'"),
                "update DOI_data = values(DOI_data)") < 0) {
              myerror += "\nError while inserting CrossRef DOIs (" + doi + ", "
                  + sdoi + "): '" + server.error() + "'";
              continue;
            }
            auto sdoi_fn = sdoi;
            replace_all(sdoi_fn, "/", "@@");
            sdoi_fn = g_config_data.tmpdir + "/" + sdoi_fn;
            if (stat((sdoi_fn + ".crossref.json").c_str(), &buf) != 0) {
              sleep_for(seconds(3));
              stringstream oss, ess;
              unixutils::mysystem2("/bin/tcsh -c \"curl -s -o " + sdoi_fn +
                  ".crossref.json 'https://api.crossref.org/works/" + sdoi +
                  "'\"", oss, ess);
              if (!ess.str().empty()) {
                myerror += "\nError while getting CrossRef citations for "
                    "subject DOI '" + sdoi + "': '" + ess.str() + "'";
                continue;
              }
            }
            auto jfil = sdoi_fn + ".crossref.json";
            ifstream ifs(jfil.c_str());
            JSON::Object sdoi_obj;
            try {
              sdoi_obj.fill(ifs);
            } catch (...) {
              myerror += "\nunable to create JSON object from '" + jfil + "'";
              continue;
            }
            ifs.close();
            if (!sdoi_obj) {
              myerror += "\nError reading CrossRef JSON for subject DOI '" +
                  sdoi + "': 'unable to create JSON object'";
              continue;
            }
            auto authlst = sdoi_obj["message"]["author"];
            for (size_t m = 0; m < authlst.size(); ++m) {
              auto family = substitute(authlst[m]["family"].to_string(), "\\",
                  "\\\\");
              auto given = authlst[m]["given"].to_string();
              string fnam, mnam;
              if (!given.empty()) {
                auto sp = split(given);
                fnam = substitute(sp.front(), "\\", "\\\\");
                if (sp.size() > 1) {
                  mnam = substitute(sp.back(), "\\", "\\\\");
                }
              }
              if (server.insert("citation.works_authors", ("ID, ID_type, "
                  "last_name, first_name, middle_name, sequence"), ("'" +
                  sdoi + "', 'DOI', '" + sql_ready(family) + "', '" + sql_ready(
                  fnam) + "', '" + sql_ready(mnam) + "', " + to_string(m)),
                  "update ID_type = values(ID_type), sequence = values("
                  "sequence)") < 0) {
                myerror += "\nError while inserting author (" + sdoi + ", DOI,"
                    + family + "," + fnam + "," + mnam + "): '" + server.
                    error() + "'";
                continue;
              }
            }
            auto typ = sdoi_obj["message"]["type"].to_string();
            if (typ == "journal-article") {
              typ = "J";
              auto pubnam = substitute(sdoi_obj["message"][
                  "short-container-title"][0].to_string(), "\\", "\\\\");
              if (server.insert("citation.journal_works", "DOI, pub_name, "
                  "volume, pages", "'" + sdoi + "', '" + pubnam + "', '" +
                  sdoi_obj["message"]["volume"].to_string() + "', '" + sdoi_obj[
                  "message"]["page"].to_string() + "'", "update pub_name = "
                  "values(pub_name), volume = values(volume), pages = values("
                  "pages)") < 0) {
                myerror += "\nError while inserting CrossRef journal data (" +
                    sdoi + "," + pubnam + "," + sdoi_obj["message"]["volume"].
                    to_string() + "," + sdoi_obj["message"]["page"].to_string()
                    + "): '" + server.error() + "'";
              }
            } else if (typ == "book-chapter") {
              typ = "C";
              auto isbn = sdoi_obj["message"]["ISBN"][0].to_string();
              if (isbn.empty()) {
                myerror += "\nError obtaining CrossRef ISBN for book chapter "
                    "(DOI: " + sdoi + ")";
                continue;
              }
              if (server.insert("citation.book_chapter_works", ("DOI, pages, "
                  "ISBN"), ("'" + sdoi + "', '" + sdoi_obj["message"]["page"].
                  to_string() + "', '" + isbn + "'"), "update pages = values("
                  "pages), ISBN = values(ISBN)") < 0) {
                myerror += "\nError while inserting book chapter data (" + sdoi
                    + "," + sdoi_obj["message"]["pages"].to_string() + "," +
                    isbn + "): '" + server.error() + "'";
                continue;
              }
              if (!inserted_book_data(isbn, server)) {
                myerror += "\nError inserting ISBN '" + isbn + "' from "
                    "CrossRef";
                continue;
              }
            } else {
              typ = "X";
            }
            auto pubyr = sdoi_obj["message"]["published-print"]["date-parts"][
                0][0].to_string();
            if (pubyr.empty()) {
              pubyr = sdoi_obj["message"]["published-online"]["date-parts"][0][
                  0].to_string();
            }
            auto ttl = substitute(sdoi_obj["message"]["title"][0].to_string(),
                "\\", "\\\\");
            auto publisher = sdoi_obj["message"]["publisher"].to_string();
            if (server.insert("citation.works", ("DOI, title, pub_year, type, "
                "publisher"), ("'" + sdoi + "', '" + ttl + "', '" + pubyr +
                "', '" + typ + "', '" + publisher + "'"), "update title = "
                "values(title), pub_year = values(pub_year), type = values("
                "type), publisher = values(publisher)") < 0) {
              myerror += "\nError while CrossRef inserting work (" + sdoi + ","
                  + ttl + "," + pubyr + "," + typ + "," + publisher + "): '" +
                  server.error() + "'";
              continue;
            }
          }
          try_error = "";
          break;
        } else {
          try_error += "\nServer failure: '" + doi_obj["message"].to_string() +
              "'\n/bin/tcsh -c \"curl -o " + filename + " 'https://"
              "api.eventdata.crossref.org/v1/events?source=crossref&obj-id=" +
              doi + "'\"";
          stringstream oss, ess;
          unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"",
              oss, ess);
        }
      } else {
        try_error += "\nServer response was not a JSON object\n/bin/tcsh -c \""
            "curl -o " + filename + " 'https://api.eventdata.crossref.org/v1/"
            "events?source=crossref&obj-id=" + doi + "'\"";
        stringstream oss, ess;
        unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"",
            oss, ess);
      }
      ++ntry;
      sleep_for(seconds(ntry * 5));
    }
    if (ntry == 3) {
      myerror += "\nError reading CrossRef JSON for DOI '" + doi + "': '" +
          try_error + "'\n/bin/tcsh -c \"curl -o " + filename + " 'https://"
          "api.eventdata.crossref.org/v1/events?source=crossref&obj-id=" + doi +
          "'\"";
    }
  }
  MySQL::LocalQuery q("select v.dsid, count(c.new_flag) from citation."
       "data_citations as c left join (select distinct dsid, doi from dssdb."
       "dsvrsn) as v on v.doi = c.DOI_data where c.new_flag = '1' group by v."
       "dsid");
  if (q.submit(server) < 0) {
    myerror += "\nError while obtaining list of new CrossRef citations: '" + q.
        error() + "'";
    return;
  }
  for (const auto& r : q) {
    g_mail_message << "\nFound " << r[1] << " new CrossRef data citations for "
        << r[0] << endl;
    stringstream oss, ess;
    unixutils::mysystem2("/bin/tcsh -c \"dsgen " + r[0].substr(2) + "\"", oss,
        ess);
    if (!ess.str().empty()) {
      myerror += "\nError while regenerating " + r[0] + ":\n  " + ess.str();
      return;
    }
  }
  string r;
  if (server.command("update citation.data_citations set new_flag = '0' where "
      "new_flag = '1'", r) < 0) {
    myerror += "\nError updating 'new_flag' in citation.data_citations: " +
        server.error();
    return;
  }
}

string cache_file(string doi) {
  string fnam = doi;
  replace_all(fnam, "/", "@@");
  fnam = g_config_data.tmpdir + "/cache/" + fnam + ".crossref.json";
  struct stat buf;
  if (stat(fnam.c_str(), &buf) != 0) {
    sleep_for(seconds(1));
    stringstream oss, ess;
    unixutils::mysystem2("/bin/tcsh -c \"curl -s -o " + fnam + " 'https://"
        "api.crossref.org/works/" + doi + "'\"", oss, ess);
    if (!ess.str().empty()) {
      myerror += "\nError while getting CrossRef authors for works DOI '" + doi
          + "': '" + ess.str() + "'";
      unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + fnam + "\"", oss,
          ess);
      return "";
    }
  }
  return fnam;
}

void fill_authors_from_cross_ref(string subj_doi, MySQL::Server& server) {
  auto fnam = cache_file(subj_doi);
  if (!fnam.empty()) {
    ifstream ifs(fnam.c_str());
    JSON::Object obj;
    try {
      obj.fill(ifs);
    } catch (...) {
      myerror += "\nunable to create JSON object from '" + fnam + "'";
      return;
    }
    if (!obj) {
      myerror += "\nError reading CrossRef author JSON for works DOI '" +
          subj_doi + "': 'unable to create JSON object'";
      stringstream oss, ess;
      unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + fnam + "\"", oss,
          ess);
      return;
    }
    auto authlst = obj["message"]["author"];
    for (size_t m = 0; m < authlst.size(); ++m) {
      auto family = substitute(authlst[m]["family"].to_string(), "\\", "\\\\");
      auto given = authlst[m]["given"].to_string();
      string fnam, mnam;
      if (!given.empty()) {
        auto sp = split(given);
        fnam = substitute(sp.front(), "\\", "\\\\");
        if (sp.size() > 1) {
          mnam = substitute(sp.back(), "\\", "\\\\");
        }
      }
      if (server.insert("citation.works_authors", ("ID, ID_type, last_name, "
          "first_name, middle_name, sequence"), ("'" + subj_doi + "', 'DOI', '"
          + sql_ready(family) + "', '" + sql_ready(fnam) + "', '" + sql_ready(
          mnam) + "', " + to_string(m)), "update ID_type = values(ID_type), "
          "sequence = values(sequence)") < 0) {
        myerror += "\nError while inserting author (" + subj_doi + ", DOI, " +
            family + ", " + fnam + ", " + mnam + "): '" + server.error() + "'";
        stringstream oss, ess;
        unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + fnam + "\"", oss,
            ess);
        return;
      }
    }
  }
}

string publisher_from_cross_ref(string subj_doi) {
  auto filename = cache_file(subj_doi);
  if (!filename.empty()) {
    ifstream ifs(filename.c_str());
    JSON::Object obj;
    try {
      obj.fill(ifs);
    } catch (...) {
      myerror += "\nunable to create JSON object from '" + filename + "'";
      return "";
    }
    if (!obj) {
      myerror += "\nError reading CrossRef publisher JSON for works DOI '" +
          subj_doi + "': 'unable to create JSON object'";
      stringstream oss, ess;
      unixutils::mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"", oss,
          ess);
      return "";
    }
    return obj["message"]["publisher"].to_string();
  }
  return "";
}

void query_elsevier(vector<string>& doi_list, MySQL::Server& server) {
  const string API_KEY = "7f662041fde0ff55f7aea7e7727763cd";
  for (const auto& doi : doi_list) {
    auto pgnum = 0;
    auto totres = 0x7fffffff;
    while (pgnum < totres) {
      auto filename = doi;
      replace_all(filename, "/", "@@");
      filename = g_config_data.tmpdir + "/" + filename + ".elsevier." + to_string(pgnum) +
          ".json";
      string url = "https://api.elsevier.com/content/search/scopus?start=" +
          to_string(pgnum) + "&query=ALL:" + doi + "&field=prism:doi,prism:url,"
          "prism:publicationName,prism:coverDate,prism:volume,prism:pageRange,"
          "prism:aggregationType,prism:isbn,dc:title&httpAccept=application/"
          "json&apiKey=" + API_KEY;
      struct stat buf;
      if (stat(filename.c_str(), &buf) != 0) {
        sleep_for(seconds(1));
        stringstream oss, ess;
        unixutils::mysystem2("/bin/tcsh -c \"curl -s -o " + filename + " '" +
            url + "'\"", oss, ess);
        if (!ess.str().empty()) {
          myerror += "\nError while getting Elsevier citations for DOI '" + doi
              + "': '" + ess.str() + "'";
          continue;
        }
      }
      ifstream ifs(filename.c_str());
      JSON::Object doi_obj;
      try {
        doi_obj.fill(ifs);
      } catch (...) {
        myerror += "\nunable to create JSON object from '" + filename + "'";
        continue;
      }
      ifs.close();
      if (!doi_obj) {
        myerror += "\nError reading Elsevier JSON for DOI '" + doi + "': '"
            "unable to create JSON object'\n/bin/tcsh -c \"curl -o " + filename
            + " '" + url + "'\"";
        continue;
      }
      try {
        totres = stoi(doi_obj["search-results"]["opensearch:"
            "totalResults"].to_string());
      } catch (const std::invalid_argument& e) {
        myerror += "\ninvalid JSON for '" + url + "'";
        continue;
      } catch (...) {
        myerror += "\nunknown error in JSON for '" + url + "'";
        continue;
      }
      if (totres > 0) {
        for (size_t n = 0; n < doi_obj["search-results"]["entry"].size(); ++n) {
          if (doi_obj["search-results"]["entry"][n]["prism:doi"].type() !=
              JSON::ValueType::Nonexistent) {
            auto sdoi = doi_obj["search-results"]["entry"][n]["prism:doi"].
                to_string();
            replace_all(sdoi,"\\/","/");
            if (server.insert("citation.data_citations", ("DOI_data, DOI_work, "
                "new_flag"), ("'" + doi + "', '" + sdoi + "', '1'"),
                "update DOI_data = values(DOI_data)") < 0) {
              myerror += "\nError while inserting Elsevier DOIs (" + doi + ", "
                  + sdoi + "): '" + server.error() + "'";
              continue;
            }
            if (doi_obj["search-results"]["entry"][n]["prism:url"].type() !=
                JSON::ValueType::Nonexistent) {
              auto sp = split(doi_obj["search-results"]["entry"][n][
                  "prism:url"].to_string(), "/");
              auto scopus_id = sp.back();
              auto authfil = g_config_data.tmpdir + "/cache/scopus_id_" + scopus_id +
                  ".elsevier.json";
              if (stat(authfil.c_str(), &buf) != 0) {
                sleep_for(seconds(1));
                stringstream oss, ess;
                url = doi_obj["search-results"]["entry"][n]["prism:url"].
                    to_string() + "?field=author,dc:publisher&httpAccept="
                    "application/json&apiKey=" + API_KEY;
                unixutils::mysystem2("/bin/tcsh -c \"curl -s -o " + authfil +
                    " '" + url + "'\"", oss, ess);
                if (!ess.str().empty()) {
                  myerror += "\nError while getting Elsevier author data for "
                      "scopus ID '" + scopus_id + "': '" + ess.str() + "'";
                  continue;
                }
              }
              ifs.open(authfil.c_str());
              JSON::Object author_obj;
              try {
                author_obj.fill(ifs);
              } catch(...) {
                myerror += "\nunable to create JSON object from '" + authfil +
                    "'";
                continue;
              }
              ifs.close();
              if (!author_obj) {
                myerror += "\nError reading Elsevier JSON for scopus id '" +
                     scopus_id + "': 'unable to create JSON object'\n/bin/tcsh "
                     "-c \"curl -o " + authfil + " '" + url + "'\"";
                continue;
              }
              auto publisher = author_obj["abstracts-retrieval-response"][
                  "coredata"]["dc:publisher"].to_string();
              if (publisher.empty()) {
                publisher = publisher_from_cross_ref(sdoi);
              }
              if (g_publisher_fixups.find(publisher) != g_publisher_fixups.end()) {
                publisher = g_publisher_fixups[publisher];
              } else {
                if (regex_search(publisher, email_re)) {
                  if (g_publisher_fixups.find(publisher) == g_publisher_fixups.
                      end()) {
                    myerror += "\n**SUSPECT PUBLISHER: '" + publisher + "'";
                  } else {
                    publisher = g_publisher_fixups[publisher];
                  }
                }
              }
              if (author_obj["abstracts-retrieval-response"]["authors"][
                  "author"].size() > 0) {
                for (size_t m = 0;  m < author_obj[
                    "abstracts-retrieval-response"]["authors"]["author"].size();
                    ++m) {
                  auto given_name = author_obj["abstracts-retrieval-response"][
                      "authors"]["author"][m]["ce:given-name"].to_string();
                  if (given_name.empty()) {
                    given_name = author_obj["abstracts-retrieval-response"][
                        "authors"]["author"][m]["preferred-name"][
                        "ce:given-name"].to_string();
                  }
                  string fnam, mnam;
                  if (!given_name.empty()) {
                    auto sp = split(given_name);
                    fnam = sp.front();
                    sp.pop_front();
                    for (const auto& i : sp) {
                      if (!mnam.empty()) {
                        mnam += " ";
                      }
                      mnam += i;
                    }
                  }
                  auto seq = author_obj["abstracts-retrieval-response"][
                      "authors"]["author"][m]["@seq"].to_string();
                  auto lnam = author_obj["abstracts-retrieval-response"][
                      "authors"]["author"][m]["ce:surname"].to_string();
                  if (server.insert("citation.works_authors", "ID, ID_type, "
                      "last_name, first_name, middle_name, sequence", "'" + sdoi
                      + "', 'DOI', '" + sql_ready(lnam) + "', '" + sql_ready(
                      fnam) + "', '" + sql_ready(mnam) + "', " + seq, "update "
                      "ID_type=values(ID_type), sequence=values(sequence)") <
                      0) {
                    myerror += "\nError while inserting author (" + sdoi +
                        ", DOI, " + lnam + ", " + fnam + ", " + mnam + ", " +
                        seq + "): '" + server.error() + "'";
                    continue;
                  }
                }
              } else {
                fill_authors_from_cross_ref(sdoi, server);
              }
              auto typ = doi_obj["search-results"]["entry"][n][
                  "prism:aggregationType"].to_string();
              if (typ == "Journal") {
                typ = "J";
                auto pubnam = doi_obj["search-results"]["entry"][n][
                    "prism:publicationName"].to_string();
                auto a = journal_abbreviation(pubnam);
                if (a == pubnam && a.find(" ") != string::npos &&
                    g_journals_no_abbreviation.find(a) == g_journals_no_abbreviation.
                    end()) {
                  myerror += "\nPublication '" + pubnam + "' has no "
                      "abbreviation and is not marked as such";
                }
                if (server.insert("citation.journal_works", ("DOI, pub_name, "
                    "volume, pages"), ("'" + sdoi + "', '" + sql_ready(a) +
                    "', '" + doi_obj["search-results"]["entry"][n][
                    "prism:volume"].to_string() + "', '" + doi_obj[
                    "search-results"]["entry"][n]["prism:pageRange"].to_string()
                    + "'"), "update pub_name = values(pub_name), volume = "
                    "values(volume), pages = values(pages)") < 0) {
                  myerror += "\nError while inserting Elsevier journal data ('"
                      + sdoi + "', '" + sql_ready(a) + "', '" +
                      doi_obj["search-results"]["entry"][n]["prism:volume"].
                      to_string() + "', '" + doi_obj["search-results"]["entry"][
                      n]["prism:pageRange"].to_string() + "'): '" + server.
                      error() + "'";
                  continue;
                }
              } else if (typ == "Book" || typ == "Book Series") {
                typ = "C";
                auto isbn = doi_obj["search-results"]["entry"][n]["prism:isbn"][
                    0]["$"].to_string();
                if (isbn.empty()) {
                  myerror += "\nError obtaining Elsevier ISBN for book chapter "
                      "(DOI: " + sdoi + ")";
                  continue;
                }
                if (server.insert("citation.book_chapter_works", ("DOI, pages, "
                    "ISBN"), ("'" + sdoi + "', '" + doi_obj["search-results"][
                    "entry"][n]["prism:pageRange"].to_string() + "', '" + isbn +
                    "'"), "update pages=values(pages), ISBN=values(ISBN)") <
                    0) {
                  myerror += "\nError while inserting book chapter data (" +
                      sdoi + ", " + doi_obj["search-results"]["entry"][n][
                      "prism:pageRange"].to_string() + ", " + isbn + "): '" +
                      server.error() + "'";
                  continue;
                }
                if (!inserted_book_data(isbn,server)) {
                  myerror += "\nError inserting ISBN '" + isbn + "' from "
                      "Elsevier";
                  continue;
                }
              } else if (typ == "Conference Proceeding") {
                typ = "P";
                if (server.insert("citation.proceedings_works", ("DOI, "
                    "pub_name, pages"), ("'" + sdoi + "', '" + doi_obj[
                    "search-results"]["entry"][n]["prism:publicationName"].
                    to_string() + "', '" + doi_obj["search-results"]["entry"][
                    n]["prism:pageRange"].to_string() + "'"), "update pub_name "
                    "= values(pub_name), pages = values(pages)") < 0) {
                  myerror += "\nError while inserting Elsevier proceedings "
                      "data (" + sdoi + ", " + doi_obj["search-results"][
                      "entry"][n]["prism:publicationName"].to_string() + ", " +
                      doi_obj["search-results"]["entry"][n]["prism:pageRange"].
                      to_string() + "): '" + server.error() + "'";
                  continue;
                }
              } else {
                typ = "X";
              }
              if (typ != "X") {
                auto pubyr = doi_obj["search-results"]["entry"][n][
                    "prism:coverDate"].to_string().substr(0, 4);
                if (!pubyr.empty()) {
                  auto ttl = sql_ready(doi_obj["search-results"]["entry"][n][
                      "dc:title"].to_string());
                  if (server.insert("citation.works", ("DOI, title, pub_year, "
                      "type, publisher"), ("'" + sdoi + "', '" + ttl + "', '" +
                      pubyr + "', '" + typ + "', '" + publisher + "'"),
                      "update title = values(title), pub_year = values("
                      "pub_year), type = values(type), publisher = values("
                      "publisher)") < 0) {
                    myerror += "\nError while Elsevier inserting work (" + sdoi
                        + ", " + ttl + ", " + pubyr + ", " + typ + ", " +
                        publisher + ") from file '" + filename + "': '" +
                        server.error() + "'";
                    continue;
                  }
                } else {
                  myerror += "\n**NO Elsevier PUBLICATION YEAR: SCOPUS ID - " +
                      scopus_id;
                }
              } else {
                myerror += "\n**UNKNOWN Elsevier TYPE: " + doi_obj[
                    "search-results"]["entry"][n]["prism:aggregationType"].
                    to_string() + " for work DOI: '" + sdoi + "' citing '" + doi
                    + "'";
              }
            } else {
              myerror += "\n**NO Elsevier SCOPUS ID: " + filename + " " +
                  doi_obj["search-results"]["entry"][n]["prism:url"].
                  to_string();
            }
          } else {
            myerror += "\n**NO Elsevier WORKS DOI:  Data DOI - " + doi +
                "  type - " + doi_obj["search-results"]["entry"][n][
                "prism:aggregationType"].to_string() + "  title - '" + doi_obj[
                "search-results"]["entry"][n]["dc:title"].to_string() + "'";
          }
        }
      }
      pgnum += stoi(doi_obj["search-results"]["opensearch:itemsPerPage"].
          to_string());
    }
  }
  MySQL::LocalQuery q("select v.dsid, count(c.new_flag) from citation."
      "data_citations as c left join (select distinct dsid, doi from dssdb."
      "dsvrsn) as v on v.doi = c.DOI_data where c.new_flag = '1' group by v."
      "dsid");
  if (q.submit(server) < 0) {
    myerror += "\nError while obtaining list of new Elsevier citations: '" +
        q.error() + "'";
    return;
  }
  for (const auto& r : q) {
    g_mail_message << "\nFound " << r[1] << " new Elsevier data citations for "
        << r[0] << endl;
    stringstream oss, ess;
    unixutils::mysystem2("/bin/tcsh -c \"dsgen "+r[0].substr(2)+"\"", oss, ess);
    if (!ess.str().empty()) {
      myerror += "\nError while regenerating " + r[0];
      return;
    }
  }
  string r;
  if (server.command("update citation.data_citations set new_flag = '0' where "
      "new_flag = '1'", r) < 0) {
    myerror += "\nError updating 'new_flag' in citation.data_citations: " +
        server.error();
    return;
  }
}

void read_config() {
  std::ifstream ifs("./citefind.cnf");
  if (!ifs.is_open()) {
    add_to_error_and_exit("unable to open configuration file");
  }
  JSON::Object o(ifs);
  ifs.close();
  if (o["temp_dir"].type() == JSON::ValueType::String) {
    g_config_data.tmpdir = o["temp_dir"].to_string();
  }
  if (g_config_data.tmpdir.empty()) {
    add_to_error_and_exit("temporary directory not specified in config");
  }
  struct stat buf;
  if (stat(g_config_data.tmpdir.c_str(), &buf) != 0) {
    add_to_error_and_exit("temporary directory '" + g_config_data.tmpdir +
        "' is missing");
  }
  auto& arr = o["centers"];
  for (size_t n = 0; n < arr.size(); ++n) {
    auto& a = arr[n];
    g_config_data.centers.emplace_back(ConfigData::Center());
    auto& c = g_config_data.centers.back();
    c.id = a["id"].to_string();
    if (a["db"].type() != JSON::ValueType::Nonexistent) {
      auto& db = a["db"];
      c.db_data.host = db["host"].to_string();
      c.db_data.username = db["username"].to_string();
      c.db_data.password = db["password"].to_string();
      c.db_data.insert_table = db["insert"].to_string();
      if (db["doi"].type() != JSON::ValueType::Nonexistent) {
        c.db_data.doi_query = db["doi"].to_string();
      }
    }
    if (a["api"].type() != JSON::ValueType::Nonexistent) {
      auto& api = a["api"];
      c.api_data.url = api["url"].to_string();
      c.api_data.doi_response = api["response"]["doi"].to_string();
    }
  }
}

void show_usage_and_exit() {
  cerr << "usage: citefind CENTER [-k]" << endl;
  cerr << "usage: citefind --help" << endl;
  cerr << "usage: citefind --show-centers" << endl;
  cerr << "\nrequired:" << endl;
  cerr << "CENTER  data center for which to get citation statistics" << endl;
  cerr << "\noptions:" << endl;
  cerr << "-k   don't clean the json files from the APIs (default is to clean)"
      << endl;
  exit(1);
}

void show_centers_and_exit() {
  cout << "Known data centers:" << endl;
  for (const auto& c : g_config_data.centers) {
    cout << "  " << c.id << endl;
  }
  cout << "\nAdd additional centers to the configuration file" << endl;
  exit(0);
}

void parse_args(int argc, char **argv) {
  string a1;
  if (argc > 1) {
    a1 = argv[1];
  }
  if (argc < 2 || a1 == "--help") {
    show_usage_and_exit();
  }
  if (a1 == "--show-centers") {
    show_centers_and_exit();
  }
  for (const auto& c : g_config_data.centers) {
    if (c.id == a1) {
      g_args.center = a1;
      break;
    }
  }
  if (g_args.center.empty()) {
    add_to_error_and_exit("center '" + a1 + "' is not configured");
  }
  for (auto n = 2; n < argc; ++n) {
    auto arg = string(argv[n]);
    if (arg == "-k") {
      g_args.clean_tmpdir = false;
    }
  }
}

void connect_to_database(MySQL::Server& server) {
  server.connect("rda-db.ucar.edu", "metadata", "metadata", "");
  if (!server) {
    add_to_error_and_exit("unable to connect to the database");
  }
}

void fill_journal_abbreviations(MySQL::Server& server) {
  MySQL::LocalQuery q("*", "citation.journal_abbreviations");
  if (q.submit(server) < 0) {
    add_to_error_and_exit("unable to get journal abbreviatons: '" + q.error() +
        "'");
  }
  for (const auto& r : q) {
    g_journal_abbreviations.emplace(r[0], r[1]);
  }
}

void fill_journals_no_abbreviation(MySQL::Server& server) {
  MySQL::LocalQuery q("*", "citation.journal_no_abbreviation");
  if (q.submit(server) < 0) {
    add_to_error_and_exit("unable to get journals with no abbrevations: '" + q.
        error() + "'");
  }
  for (const auto& r : q) {
    g_journals_no_abbreviation.emplace(r[0]);
  }
}

void fill_publisher_fixups(MySQL::Server& server) {
  MySQL::LocalQuery q("*", "citation.publisher_fixups");
  if (q.submit(server) < 0) {
    add_to_error_and_exit("unable to get publisher fixups: '" + q.error() +
        "'");
  }
  for (const auto& r : q) {
    g_publisher_fixups.emplace(r[0], r[1]);
  }
}

void fill_doi_list_from_db(const ConfigData::Center& c, vector<string>&
    doi_list) {
  MySQL::Server srv(c.db_data.host, c.db_data.username, c.db_data.password, "");
  if (!srv) {
    add_to_error_and_exit("unable to connect to MySQL server for the DOI list");
  }
  MySQL::LocalQuery q(c.db_data.doi_query);
  if (q.submit(srv) < 0) {
    add_to_error_and_exit("mysql error '" + q.error() + "' while getting the "
        "DOI list");
  }
  doi_list.reserve(q.num_rows());
  for (const auto& r : q) {
    doi_list.emplace_back(r[0]);
  }
}

void fill_doi_list(vector<string>& doi_list) {
  for (const auto& c : g_config_data.centers) {
    if (c.id == g_args.center) {
      if (!c.db_data.doi_query.empty()) {
        fill_doi_list_from_db(c, doi_list);
      } else if (!c.api_data.url.empty()) {
      } else {
        add_to_error_and_exit("can't figure out how to get the list of DOIs "
            "from the current configuration");
      }
    }
    break;
  }
}

void print_publisher_list(MySQL::Server& server) {
  MySQL::LocalQuery q("distinct publisher", "citation.works");
  if (q.submit(server) < 0) {
    add_to_error_and_exit("unable to get list of pubishers from 'works' table: "
        "'" + q.error() + "'");
  }
  g_myoutput << "\nCurrent Publisher List:" << endl;
  for (const auto& r : q) {
    g_myoutput << "Publisher: '" << r[0] << "'" << endl;
  }
}

int main(int argc, char **argv) {
  atexit(clean_up);
  read_config();
  parse_args(argc, argv);
  MySQL::Server srv;
  connect_to_database(srv);
  fill_journal_abbreviations(srv);
  fill_journals_no_abbreviation(srv);
  fill_publisher_fixups(srv);
  vector<string> doi_list;
  fill_doi_list(doi_list);
for (const auto& e : doi_list) {
cerr << e << endl;
}
exit(1);
  query_crossref(doi_list, srv);
  query_elsevier(doi_list, srv);
  print_publisher_list(srv);
  srv.disconnect();
  exit(0);
}
