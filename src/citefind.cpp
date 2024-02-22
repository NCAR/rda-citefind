#include "../include/citefind.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <sys/stat.h>
#include <PostgreSQL.hpp>
#include <json.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tempfile.hpp>

using namespace PostgreSQL;
using std::cerr;
using std::chrono::seconds;
using std::cout;
using std::deque;
using std::endl;
using std::find;
using std::get;
using std::ifstream;
using std::make_tuple;
using std::regex;
using std::regex_search;
using std::stoi;
using std::string;
using std::stringstream;
using std::tie;
using std::this_thread::sleep_for;
using std::to_string;
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::sql_ready;
using strutils::substitute;
using strutils::to_lower;
using strutils::to_title;
using strutils::trim;
using unixutils::mysystem2;

string myerror = "";
string mywarning = "";
citefind::ConfigData g_config_data;
citefind::Args g_args;
Server g_server;
stringstream g_myoutput, g_mail_message;
unordered_map<string, string> g_journal_abbreviations, g_publisher_fixups;
unordered_set<string> g_journals_no_abbreviation;
const regex email_re("(.)*@(.)*\\.(.)*");
std::ofstream g_output;
string g_single_doi;

typedef tuple<string, string, string> DOI_DATA;
typedef vector<DOI_DATA> DOI_LIST;
unordered_map<string, citefind::SERVICE_DATA> g_services;

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

string repair_string(string s) {
  auto sp = split(s, "\\n");
  if (sp.size() > 1) {
    s.clear();
    for (auto& p : sp) {
      trim(p);
      append(s, p, " ");
    }
  }
  replace_all(s, "\\/", "/");
  replace_all(s, "\\", "\\\\");
  return s;
}

void get_citations(string url, string service_id, size_t sleep, string
    filename, JSON::Object& doi_obj) {
  struct stat buf;
  if (stat(filename.c_str(), &buf) != 0) {
    sleep_for(seconds(3));
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"curl -s -o " + filename + " '" + url + "'\"", oss,
        ess);
    if (!ess.str().empty()) {
      append(myerror, "Error while getting " + service_id + " citations from '"
          + url + "': '" + ess.str() + "'", "\n");
    }
  }
  ifstream ifs(filename.c_str());
  try {
    doi_obj.fill(ifs);
  } catch (...) {
    append(myerror, "unable to create JSON object from file '" + filename + "'",
        "\n");
  }
  ifs.close();
}

string cache_file(string doi) {
  string fnam = doi;
  replace_all(fnam, "/", "@@");
  fnam = g_config_data.tmpdir + "/cache/" + fnam + ".crossref.json";
  struct stat buf;
  if (stat(fnam.c_str(), &buf) != 0) {
    sleep_for(seconds(3));
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"curl -s -o '" + fnam + "' 'https://api.crossref."
        "org/works/" + doi + "'\"", oss, ess);
    if (!ess.str().empty()) {
      append(myerror, "Error while getting CrossRef data for works DOI '" + doi
          + "': '" + ess.str() + "'", "\n");
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + fnam + "\"", oss, ess);
      return "";
    }
  }
  return fnam;
}

bool filled_authors_from_cross_ref(string subj_doi, JSON::Object& obj) {
  auto cfile = cache_file(subj_doi);
  if (!cfile.empty()) {
    ifstream ifs(cfile.c_str());
    try {
      obj.fill(ifs);
    } catch (...) {
      append(myerror, "unable to create JSON object from '" + cfile + "'",
          "\n");
      return false;
    }
    ifs.close();
    if (!obj) {
      append(myerror, "Error reading CrossRef author JSON for works DOI '" +
          subj_doi + "': 'unable to create JSON object'", "\n");
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + cfile + "\"", oss, ess);
      return false;
    }
    auto authlst = obj["message"]["author"];
    for (size_t m = 0; m < authlst.size(); ++m) {
      auto family = citefind::convert_unicodes(substitute(authlst[m]["family"].
          to_string(), "\\", "\\\\"));
      auto given = citefind::convert_unicodes(authlst[m]["given"].to_string());
      if (!given.empty()) {
        replace_all(given, ".-", ". -");
        auto sp = split(given);
        auto fname = substitute(sp.front(), "\\", "\\\\");
        string mname;
        for (size_t n = 1; n < sp.size(); ++n) {
          append(mname, substitute(sp[n], "\\", "\\\\"), " ");
        }
        auto orcid = authlst[m]["ORCID"].to_string();
        if (orcid.empty()) {
          orcid = "NULL";
        } else {
          if (orcid.find("http") == 0) {
            auto idx = orcid.rfind("/");
            if (idx != string::npos) {
              orcid = orcid.substr(idx + 1);
            }
          }
        }
        if (!citefind::inserted_works_author(subj_doi, "DOI", fname, mname,
            family, orcid, m, "CrossRef")) {
          stringstream oss, ess;
          mysystem2("/bin/tcsh -c \"/bin/rm -f " + cfile + "\"", oss, ess);
          return false;
        }
      }
    }
  }
  return true;
}

bool filled_authors_from_scopus(string scopus_url, string api_key, string
    subj_doi, JSON::Object& author_obj) {
  auto sp = split(scopus_url, "/");
  auto scopus_id = sp.back();
  auto authfil = g_config_data.tmpdir + "/cache/scopus_id_" + scopus_id +
      ".elsevier.json";
  auto url = scopus_url + "?field=author,dc:publisher&httpAccept=application/"
      "json&apiKey=" + api_key;
  struct stat buf;
  if (stat(authfil.c_str(), &buf) != 0) {
    sleep_for(seconds(1));
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"curl -s -o " + authfil + " '" + url + "'\"", oss,
        ess);
    if (!ess.str().empty()) {
      append(myerror, "Error while getting Elsevier author data for scopus ID '"
          + scopus_id + "': '" + ess.str() + "'", "\n");
      return false;
    }
  }
  std::ifstream ifs(authfil.c_str());
  try {
    author_obj.fill(ifs);
  } catch(...) {
    append(myerror, "unable to create JSON object from '" + authfil + "'",
        "\n");
    return false;
  }
  ifs.close();
  if (!author_obj) {
    append(myerror, "Error reading Elsevier JSON for scopus id '" + scopus_id +
        "': 'unable to create JSON object'\n/bin/tcsh -c \"curl -o " + authfil +
        " '" + url + "'\"", "\n");
    return false;
  }
  if (author_obj["abstracts-retrieval-response"]["authors"]["author"].size() >
      0) {
    for (size_t m = 0;  m < author_obj["abstracts-retrieval-response"][
        "authors"]["author"].size(); ++m) {
      auto given_name = author_obj["abstracts-retrieval-response"]["authors"][
          "author"][m]["ce:given-name"].to_string();
      if (given_name.empty()) {
        given_name = author_obj["abstracts-retrieval-response"]["authors"][
            "author"][m]["preferred-name"]["ce:given-name"].to_string();
      }
      string fnam, mnam;
      if (!given_name.empty()) {
        replace_all(given_name, ".-", ". -");
        auto sp = split(given_name);
        fnam = sp.front();
        sp.pop_front();
        for (const auto& i : sp) {
          append(mnam, i, " ");
        }
      }
      auto seq = author_obj["abstracts-retrieval-response"]["authors"][
          "author"][m]["@seq"].to_string();
      auto lnam = author_obj["abstracts-retrieval-response"]["authors"][
          "author"][m]["ce:surname"].to_string();
      if (!citefind::inserted_works_author(subj_doi, "DOI", fnam, mnam, lnam,
          "", stoi(seq), "Scopus")) {
        return false;
      }
    }
    return true;
  }
  return false;
}

void regenerate_dataset_descriptions(string whence) {
  LocalQuery q("select v.dsid, count(c.new_flag) from citation.data_citations "
      "as c left join (select distinct dsid, doi from dssdb.dsvrsn) as v on v."
      "doi ilike c.doi_data where c.new_flag = '1' group by v.dsid");
  if (q.submit(g_server) < 0) {
    append(myerror, "Error while obtaining list of new " + whence +
        " citations: '" + q.error() + "'", "\n");
    return;
  }
  for (const auto& r : q) {
    g_output << "\nFound " << r[1] << " new " << whence << " data citations "
        "for " << r[0] << endl;
    g_mail_message << "\nFound " << r[1] << " new " << whence << " data "
        "citations for " << r[0] << endl;
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"dsgen " + r[0].substr(2) + "\"", oss, ess);
    if (!ess.str().empty()) {
      append(myerror, "Error while regenerating " + r[0] + " (" + whence + "):"
          "\n  " + ess.str(), "\n");
    }
  }
}

void reset_new_flag() {
  if (g_server.command("update " + g_args.doi_group.insert_table + " set "
      "new_flag = '0' where new_flag = '1'") < 0) {
    append(myerror, "Error updating 'new_flag' in " + g_args.doi_group.
        insert_table + ": " + g_server.error(), "\n");
    return;
  }
}

size_t try_crossref(const DOI_DATA& doi_data, const citefind::SERVICE_DATA&
    service_data, string& try_error) {
  static const string API_URL = get<2>(service_data);
  string doi, publisher, asset_type;
  tie(doi, publisher, asset_type) = doi_data;
  size_t ntries = 0;
  while (ntries < 3) {
    sleep_for(seconds(ntries * 5));
    ++ntries;
    auto filename = doi;
    replace_all(filename, "/", "@@");
    filename = g_config_data.tmpdir + "/" + filename + ".crossref.json";
    string url = API_URL + "?source=crossref&obj-id=" + doi;
    JSON::Object doi_obj;
    get_citations(url, get<0>(service_data), 3, filename, doi_obj);
    if (!doi_obj) {
      try_error += "\nServer response was not a JSON object\n/bin/tcsh -c \""
          "curl -o " + filename + " '" + API_URL + "?source=crossref&obj-id=" +
          doi + "'\"";
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"", oss, ess);
      continue;
    }
    if (doi_obj["status"].to_string() != "ok") {
      try_error += "\nServer failure: '" + doi_obj["message"].to_string() +
          "'\n/bin/tcsh -c \"curl -o " + filename + " '" + API_URL + "?source="
          "crossref&obj-id=" + doi + "'\"";
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"", oss, ess);
      continue;
    }
    g_output << "      " << doi_obj["message"]["events"].size() << " citations "
        "found ..." << endl;
    for (size_t n = 0; n < doi_obj["message"]["events"].size(); ++n) {

      // get the "works" DOI
      auto sid = doi_obj["message"]["events"][n]["subj_id"].to_string();
      replace_all(sid, "\\/", "/");
      auto sp = split(sid, "doi.org/");
      auto sdoi = sp.back();
      if (!citefind::inserted_citation(doi, sdoi, get<0>(service_data))) {
        continue;
      }
      citefind::insert_source(sdoi, doi, get<0>(service_data));
      if (!citefind::inserted_doi_data(doi, publisher, asset_type, get<0>(
          service_data))) {
        continue;
      }
      if (g_args.no_works) {
        continue;
      }

      // add the author data for the citing "work"
      JSON::Object sdoi_obj;
      if (!filled_authors_from_cross_ref(sdoi, sdoi_obj)) {
        continue;
      }

      // get the type of the "work" and add type-specific data
      auto typ = sdoi_obj["message"]["type"].to_string();
      if (typ == "journal-article") {
        typ = "J";
        auto pubnam = substitute(sdoi_obj["message"]["container-title"][0]
            .to_string(), "\\", "\\\\");
        if (pubnam.empty()) {
          pubnam = substitute(sdoi_obj["message"]["short-container-title"][0]
            .to_string(), "\\", "\\\\");
        }
        auto& e = sdoi_obj["message"];
        auto vol = e["volume"].to_string();
        auto iss = e["issue"].to_string();
        if (!iss.empty()) {
            vol += "(" + iss + ")";
        }
        citefind::inserted_journal_works_data(sdoi, pubnam, vol, e["page"].
            to_string(), get<0>(service_data));
      } else if (typ == "book-chapter") {
        typ = "C";
        auto isbn = sdoi_obj["message"]["ISBN"][0].to_string();
        if (isbn.empty()) {
          append(myerror, "Error obtaining CrossRef ISBN for book chapter "
              "(DOI: " + sdoi + ")", "\n");
          continue;
        }
        if (!citefind::inserted_book_chapter_works_data(sdoi, sdoi_obj[
            "message"]["page"].to_string(), isbn, get<0>(service_data))) {
          continue;
        }
        if (!citefind::inserted_book_data(isbn)) {
          append(myerror, "Error inserting ISBN '" + isbn + "' from CrossRef",
              "\n");
          continue;
        }
      } else if (typ == "proceedings-article" || (typ == "posted-content" &&
          sdoi_obj["message"]["subtype"].to_string() == "preprint")) {
        typ = "P";
        auto pubnam = substitute(sdoi_obj["message"]["container-title"][0].
            to_string(), "\\", "\\\\");
        if (pubnam.empty()) {
          pubnam = substitute(sdoi_obj["message"]["short-container-title"][0].
              to_string(), "\\", "\\\\");
        }
        if (!citefind::inserted_proceedings_works_data(doi, pubnam, "", "", get<
            0>(service_data))) {
          continue;
        }
      } else {
        append(myerror, "**UNKNOWN CrossRef TYPE: " + typ + " for work DOI: '" +
            sdoi + "' citing '" + doi + "'", "\n");
        continue;
      }

      // add general data about the "work"
      auto pubyr = sdoi_obj["message"]["published-print"]["date-parts"][0][0].
          to_string();
      if (pubyr.empty()) {
        if (typ == "P") {
          pubyr = sdoi_obj["message"]["issued"]["date-parts"][0][0].to_string();
          auto sp = split(pubyr, ",");
          pubyr = sp.front();
        } else {
          pubyr = sdoi_obj["message"]["published-online"]["date-parts"][0][0].
              to_string();
        }
      }
      auto ttl = citefind::convert_unicodes(repair_string(sdoi_obj["message"][
          "title"][0].to_string()));
      auto publisher = sdoi_obj["message"]["publisher"].to_string();
      if (!citefind::inserted_general_works_data(sdoi, ttl, pubyr, typ,
          publisher, get<0>(service_data), "")) {
        continue;
      }
    }
    try_error = "";
    break;
  }
  return ntries;
}

void query_crossref(const DOI_LIST& doi_list, const citefind::SERVICE_DATA&
    service_data) {
  string doi, publisher, asset_type;
  for (const auto& e : doi_list) {
    tie(doi, publisher, asset_type) = e;
    g_output << "    querying DOI '" << doi << " | " << publisher << " | " <<
        asset_type << "' ..." << endl;
    string try_error;
    auto ntries = try_crossref(e, service_data, try_error);
    if (ntries == 3) {
      append(myerror, "Error reading CrossRef JSON for DOI '" + doi + "': '" +
          try_error + "'\n/bin/tcsh -c \"curl '" + get<2>(service_data) +
          "?source=crossref&obj-id=" + doi + "'\"", "\n");
    }
  }
  if (g_args.doi_group.id == "rda") {
    regenerate_dataset_descriptions(get<0>(service_data));
  }
  reset_new_flag();
}

string publisher_from_cross_ref(string subj_doi) {
  auto filename = cache_file(subj_doi);
  if (!filename.empty()) {
    ifstream ifs(filename.c_str());
    JSON::Object obj;
    try {
      obj.fill(ifs);
    } catch (...) {
      append(myerror, "unable to create JSON object from '" + filename + "'",
          "\n");
      return "";
    }
    if (!obj) {
      append(myerror, "Error reading CrossRef publisher JSON for works DOI '" +
          subj_doi + "': 'unable to create JSON object'", "\n");
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"", oss, ess);
      return "";
    }
    return obj["message"]["publisher"].to_string();
  }
  return "";
}

void query_elsevier(const DOI_LIST& doi_list, const citefind::SERVICE_DATA&
    service_data) {
  string doi, publisher, asset_type;
  for (const auto& e : doi_list) {
    tie(doi, publisher, asset_type) = e;
    g_output << "    querying DOI '" << doi << " | " << publisher << " | " <<
        asset_type << "' ..." << endl;
    auto pgnum = 0;
    auto totres = 0x7fffffff;
    while (pgnum < totres) {
      auto filename = doi;
      replace_all(filename, "/", "@@");
      filename = g_config_data.tmpdir + "/" + filename + ".elsevier." +
          to_string(pgnum) + ".json";
      string url = get<2>(service_data) + "?start=" + to_string(pgnum) +
          "&query=ALL:" + doi + "&field=prism:doi,prism:url,prism:"
          "publicationName,prism:coverDate,prism:volume,prism:pageRange,prism:"
          "aggregationType,prism:isbn,dc:title&httpAccept=application/json&"
          "apiKey=" + get<3>(service_data);
      JSON::Object doi_obj;
      size_t num_tries = 0;
      for (; num_tries < 3; ++num_tries) {
        sleep_for(seconds(num_tries * 5));
        get_citations(url, get<0>(service_data), 1, filename, doi_obj);
        if (doi_obj && doi_obj["service-error"].type() == JSON::ValueType::
            Nonexistent) {
          break;
        } else {
          stringstream oss, ess;
          mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"", oss, ess);
        }
      }
      if (num_tries == 3) {
        append(myerror, "Error reading Elsevier JSON for DOI '" + doi + "': '"
            "unable to create JSON object'\n/bin/tcsh -c \"curl -o " + filename
            + " '" + url + "'\"", "\n");
        continue;
      }
      try {
        totres = stoi(doi_obj["search-results"]["opensearch:totalResults"].
            to_string());
      } catch (const std::invalid_argument& e) {
        append(myerror, "invalid JSON for '" + url + "'", "\n");
        continue;
      } catch (...) {
        append(myerror, "unknown error in JSON for '" + url + "'", "\n");
        continue;
      }
      pgnum += stoi(doi_obj["search-results"]["opensearch:itemsPerPage"].
          to_string());
      if (totres == 0) {
        continue;
      }
      for (size_t n = 0; n < doi_obj["search-results"]["entry"].size(); ++n) {

        // get the "works" DOI
        if (doi_obj["search-results"]["entry"][n]["prism:doi"].type() == JSON::
            ValueType::Nonexistent) {
          append(myerror, "**NO Elsevier WORKS DOI:  Data DOI - " + doi +
              "  type - " + doi_obj["search-results"]["entry"][n][
              "prism:aggregationType"].to_string() + "  title - '" + doi_obj[
              "search-results"]["entry"][n]["dc:title"].to_string() + "'",
              "\n");
          continue;
        }
        auto sdoi = doi_obj["search-results"]["entry"][n]["prism:doi"].
            to_string();
        replace_all(sdoi, "\\/","/");
        if (!citefind::inserted_citation(doi, sdoi, get<0>(service_data))) {
          continue;
        }
        citefind::insert_source(sdoi, doi, get<0>(service_data));
        if (!citefind::inserted_doi_data(doi, publisher, asset_type, get<0>(
            service_data))) {
          continue;
        }
        if (g_args.no_works) {
          continue;
        }

        // add the author data for the citing "work"
        auto prism_url = doi_obj["search-results"]["entry"][n]["prism:url"];
        if (prism_url.type() == JSON::ValueType::Nonexistent) {
          append(myerror, "**NO Elsevier SCOPUS ID: " + filename + " " +
              prism_url.to_string(), "\n");
          continue;
        }
        auto scopus_url = prism_url.to_string();
        JSON::Object author_obj;
        if (!filled_authors_from_scopus(scopus_url, get<3>(service_data), sdoi,
            author_obj)) {
          JSON::Object cr_obj;
          filled_authors_from_cross_ref(sdoi, cr_obj);
        }

        // get the type of the "work" and add type-specific data
        auto typ = doi_obj["search-results"]["entry"][n][
            "prism:aggregationType"].to_string();
        if (typ == "Journal") {
          typ = "J";
          auto pubnam = doi_obj["search-results"]["entry"][n][
              "prism:publicationName"].to_string();
          if (!citefind::inserted_journal_works_data(sdoi, pubnam, doi_obj[
              "search-results"]["entry"][n]["prism:volume"].to_string(),
              doi_obj["search-results"]["entry"][n]["prism:pageRange"].
              to_string(), get<0>(service_data))) {
            continue;
          }
        } else if (typ == "Book" || typ == "Book Series") {
          typ = "C";
          auto isbn = doi_obj["search-results"]["entry"][n]["prism:isbn"][0][
              "$"].to_string();
          if (isbn.empty()) {
            append(myerror, "Error obtaining Elsevier ISBN for book chapter "
                "(DOI: " + sdoi + ")", "\n");
            continue;
          }
          if (!citefind::inserted_book_chapter_works_data(sdoi, doi_obj[
              "search-results"]["entry"][n]["prism:pageRange"].to_string(),
              isbn, get<0>(service_data))) {
            continue;
          }
          if (!citefind::inserted_book_data(isbn)) {
            append(myerror, "Error inserting ISBN '" + isbn + "' from Elsevier",
                "\n");
            continue;
          }
        } else if (typ == "Conference Proceeding") {
          typ = "P";
          auto& e = doi_obj["search-results"]["entry"][n];
          if (!citefind::inserted_proceedings_works_data(sdoi, e[
              "prism:publicationName"].to_string(), e["prism:volume"].
              to_string(), e["prism:pageRange"].to_string(), get<0>(
              service_data))) {
            continue;
          }
        } else {
          append(myerror, "**UNKNOWN Elsevier TYPE: " + typ + " for work DOI: '"
              + sdoi + "' citing '" + doi + "'", "\n");
          continue;
        }

        // add general data about the "work"
        auto pubyr = doi_obj["search-results"]["entry"][n]["prism:coverDate"].
            to_string().substr(0, 4);
        if (!pubyr.empty()) {
          auto ttl = repair_string(doi_obj["search-results"]["entry"][n][
              "dc:title"].to_string());
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
                append(myerror, "**SUSPECT PUBLISHER: '" + publisher + "'",
                    "\n");
              } else {
                publisher = g_publisher_fixups[publisher];
              }
            }
          }
          if (!citefind::inserted_general_works_data(sdoi, ttl, pubyr, typ,
              publisher, get<0>(service_data), scopus_url)) {
            continue;
          }
        } else {
          append(myerror, "**NO Elsevier PUBLICATION YEAR: SCOPUS URL - " +
              scopus_url, "\n");
        }
      }
    }
  }
  if (g_args.doi_group.id == "rda") {
    regenerate_dataset_descriptions(get<0>(service_data));
  }
  reset_new_flag();
}

tuple<string, string> parse_author_first_name(string fname) {
  tuple<string, string> t; // return value
  if (!fname.empty()) {
    auto sp = split(fname);
    get<0>(t) = sp.front();
    sp.pop_front();
    for (const auto& e : sp) {
      append(get<1>(t), e, " ");
    }
  }
  return t;
}

void fill_authors_from_wos(string doi_work, const JSON::Value& v) {
  vector<tuple<string, string, string, string, size_t>> author_names;
  if (v.type() == JSON::ValueType::Array) {
    for (size_t n = 0; n < v.size(); ++n) {
      auto& names = v[n]["names"]["name"];
      if (names.type() == JSON::ValueType::Array) {
        for (size_t m = 0; m < names.size(); ++m) {
          string fname, mname;
          tie(fname, mname) = parse_author_first_name(names[m]["first_name"].
              to_string());
          author_names.emplace_back(make_tuple(fname, mname, names[m][
              "last_name"].to_string(), names[m]["orcid_id"].to_string(), stoi(
              names[m]["seq_no"].to_string()) - 1));
        }
      } else {
        string fname, mname;
        tie(fname, mname) = parse_author_first_name(names["first_name"].
            to_string());
        author_names.emplace_back(make_tuple(fname, mname, names["last_name"].
            to_string(), names["orcid_id"].to_string(), stoi(names["seq_no"].
            to_string()) - 1));
      }
    }
  } else {
    auto& names = v["names"]["name"];
    if (names.type() == JSON::ValueType::Array) {
      for (size_t m = 0; m < names.size(); ++m) {
        string fname, mname;
        tie(fname, mname) = parse_author_first_name(names[m]["first_name"].
            to_string());
        author_names.emplace_back(make_tuple(fname, mname, names[m][
            "last_name"].to_string(), names[m]["orcid_id"].to_string(), stoi(
            names[m]["seq_no"].to_string()) - 1));
      }
    } else {
      string fname, mname;
      tie(fname, mname) = parse_author_first_name(names["first_name"].
          to_string());
      author_names.emplace_back(make_tuple(fname, mname, names["last_name"].
          to_string(), names["orcid_id"].to_string(), stoi(names["seq_no"].
          to_string()) - 1));
    }
  }
  for (const auto& name : author_names) {
    citefind::inserted_works_author(doi_work, "DOI", get<0>(name), get<1>(name),
        get<2>(name), get<3>(name), get<4>(name), "WoS");
  }
}

void query_wos(const DOI_LIST& doi_list, const citefind::SERVICE_DATA&
    service_data) {
//return;
  static const string API_URL = get<2>(service_data);
  static const string API_KEY_HEADER = "X-ApiKey: " + get<3>(service_data);
  string doi, publisher, asset_type;
  for (const auto& e : doi_list) {
    tie(doi, publisher, asset_type) = e;
if (to_lower(doi) == "10.5065/8a4y-cg39") {
continue;
}
    g_output << "    querying DOI '" << doi << " | " << publisher << " | " <<
        asset_type << "' ..." << endl;

    // get the WoS ID for the DOI
    stringstream oss, ess;
std::cerr << "/bin/tcsh -c \"curl -H '" + API_KEY_HEADER + "' '" + API_URL + "/?databaseId=DCI&usrQuery=DO=" + doi + "&count=1&firstRecord=1&viewField=none'\"" << std::endl;
    if (mysystem2("/bin/tcsh -c \"curl -H '" + API_KEY_HEADER + "' '" + API_URL
        + "/?databaseId=DCI&usrQuery=DO=" + doi + "&count=1&firstRecord=1&"
        "viewField=none'\"", oss, ess) < 0) {
      append(myerror, "Error getting WoS ID for DOI '" + doi + "'", "\n");
      continue;
    }
    JSON::Object o(oss.str());
    if (!o) {
      append(myerror, "Wos response for DOI '" + doi + "' ID is not JSON",
          "\n");
      continue;
    }
    auto wos_id = o["Data"]["Records"]["records"]["REC"][0]["UID"].to_string();
    if (wos_id.empty()) {
      g_output << "      No WoS ID found" << endl;
      continue;
    }
    g_output << "      WoS ID: '" << wos_id << "'" << endl;

    // get the WoS IDs for the "works" that have cited this DOI
    vector<string> works_ids;
    auto count = 100;
    auto first_rec = 1;
    auto num_records = 2;
    while (first_rec < num_records) {
      if (mysystem2("/bin/tcsh -c \"curl -H '" + API_KEY_HEADER + "' '" +
          API_URL + "/citing?databaseId=WOS&uniqueId=" + wos_id + "&count=" +
          to_string(count) + "&firstRecord=" + to_string(first_rec) +
          "&viewField='\"", oss, ess) < 0) {
        append(myerror, "Error getting WoS citation IDs for DOI '" + doi + "'",
            "\n");
        continue;
      }
      o.fill(oss.str());
      auto& arr = o["Data"]["Records"]["records"]["REC"];
      if (arr.type() != JSON::ValueType::Nonexistent) {
        for (size_t n = 0; n < arr.size(); ++n) {
          works_ids.emplace_back(arr[n]["UID"].to_string());
        }
        num_records = stoi(o["QueryResult"]["RecordsFound"].to_string());
      }
      first_rec += count;
    }
    g_output << "      " << works_ids.size() << " citations found ..." <<
        endl;
    for (const auto& work_id : works_ids) {
      string view_field = "identifiers";
      if (!g_args.no_works) {
        view_field += "+names+titles+pub_info";
      }

      // get the data for each "work"
      auto cache_fn = g_config_data.tmpdir + "/cache/" + work_id + ".json";
      struct stat buf;
      if (stat(cache_fn.c_str(), &buf) != 0) {
        if (mysystem2("/bin/tcsh -c \"curl -H '" + API_KEY_HEADER + "' -s -o " +
            cache_fn + " '" + API_URL + "/id/" + work_id + "?databaseId=WOS&"
            "count=1&firstRecord=1&viewField=" + view_field + "'\"", oss, ess) <
            0) {
          append(myerror, "Error get WoS work data for ID '" + work_id + "' "
              "(DOI " + doi + ")", "\n");
          continue;
        }
      }
      ifstream ifs(cache_fn.c_str());
      o.fill(ifs);
      if (!o) {
        append(myerror, "Error - '" + cache_fn + "' is not a JSON file (DOI " +
            doi + ")", "\n");
        continue;
      }
      auto& record = o["Data"]["Records"]["records"]["REC"][0];
      auto& identifiers = record["dynamic_data"]["cluster_related"][
          "identifiers"]["identifier"];
      string doi_work;
      for (size_t n = 0; n < identifiers.size(); ++n) {
        if (identifiers[n]["type"].to_string() == "doi") {
          doi_work = identifiers[n]["value"].to_string();
          break;
        }
      }
      if (doi_work.empty()) {
        append(myerror, "Missing work DOI for WoS ID '" + work_id + "' (DOI " +
            doi + ")", "\n");
        continue;
      }
      g_output << "        WoS ID: '" << work_id << "', DOI: '" << doi_work <<
          "'" << endl;
      if (!citefind::inserted_citation(doi, doi_work, get<0>(service_data))) {
        continue;
      }
      citefind::insert_source(doi_work, doi, get<0>(service_data));
      if (!citefind::inserted_doi_data(doi, publisher, asset_type, get<0>(
          service_data))) {
        continue;
      }
      if (g_args.no_works) {
        continue;
      }

      // add author data for the citing "work"
      auto& summary = record["static_data"]["summary"];
      fill_authors_from_wos(doi_work, summary);
      auto& address_names = record["static_data"]["fullrecord_metadata"][
          "addresses"]["address_name"];
      fill_authors_from_wos(doi_work, address_names);

      // get the type of the "work" and add type-specific data
      auto& pub_info = summary["pub_info"];
      auto pubtype = pub_info["pubtype"].to_string();
      string item_title;
      if (pubtype == "Journal") {
        pubtype = "J";
        auto& titles = summary["titles"]["title"];
        string pubnam;
        for (size_t n = 0; n < titles.size(); ++n) {
          auto type = titles[n]["type"].to_string();
          if (type == "source") {
            pubnam = to_title(titles[n]["content"].to_string());
          } else if (type == "item") {
            item_title = titles[n]["content"].to_string();
          }
        }
        if (pubnam.empty()) {
          append(myerror, "Unable to find WoS publication name (" + work_id +
              ")", "\n");
          continue;
        }
        auto vol = pub_info["vol"].to_string();
        auto issue = pub_info["issue"].to_string();
        if (!issue.empty()) {
          vol += "(" + issue + ")";
        }
        if (!citefind::inserted_journal_works_data(doi_work, pubnam, vol,
            pub_info["page"]["content"].to_string(), get<0>(service_data))) {
          continue;
        }
      } else {
        append(myerror, "Unknown WoS pubtype '" + pubtype + "' (" + work_id +
            ")", "\n");
        continue;
      }
      if (item_title.empty()) {
        append(myerror, "Unable to find WoS item title (" + work_id + ")",
            "\n");
        continue;
      }

      // add general data about the "work"
      auto pubyr = pub_info["pubyear"].to_string();
      if (!pubyr.empty()) {
        auto& p = summary["publishers"]["publisher"]["names"]["name"];
        auto publisher = p["unified_name"].to_string();
        if (publisher.empty()) {
          publisher = to_title(p["full_name"].to_string());
        }
        if (publisher.empty()) {
          append(myerror, "**NO WoS PUBLISHER (" + work_id + ")", "\n");
          continue;
        }
        if (!citefind::inserted_general_works_data(doi_work, item_title, pubyr,
            pubtype, publisher, get<0>(service_data), work_id)) {
          continue;
        }
      } else {
        append(myerror, "**NO WoS PUBLICATION YEAR (" + work_id + ")", "\n");
      }
    }
  }
  if (g_args.doi_group.id == "rda") {
    regenerate_dataset_descriptions(get<0>(service_data));
  }
  reset_new_flag();
}

void clean_cache() {
  stringstream oss, ess;
  if (mysystem2("/bin/tcsh -c 'find " + g_config_data.tmpdir + "/cache/* "
      "-mtime +180 -exec rm {} \\;'", oss, ess) != 0) {
    citefind::add_to_error_and_exit("unable to clean cache - error: '" + ess.
        str() + "'");
  }
}

void create_doi_table() {
  if (!table_exists(g_server, g_args.doi_group.insert_table)) {
    if (g_server.command("create table " + g_args.doi_group.insert_table
        + " like citation.template_data_citations") < 0) {
      citefind::add_to_error_and_exit("unable to create citation table '" +
          g_args.doi_group.insert_table + "'; error: '" + g_server.error() +
          "'");
    }
  }
}

void fill_journal_abbreviations() {
  LocalQuery q("word, abbreviation", "citation.journal_abbreviations");
  if (q.submit(g_server) < 0) {
    citefind::add_to_error_and_exit("unable to get journal abbreviatons: '" + q.
        error() + "'");
  }
  for (const auto& r : q) {
    g_journal_abbreviations.emplace(r[0], r[1]);
  }
}

void fill_journals_no_abbreviation() {
  LocalQuery q("full_name", "citation.journal_no_abbreviation");
  if (q.submit(g_server) < 0) {
    citefind::add_to_error_and_exit("unable to get journals with no "
        "abbrevations: '" + q.error() + "'");
  }
  for (const auto& r : q) {
    g_journals_no_abbreviation.emplace(r[0]);
  }
}

void fill_publisher_fixups() {
  LocalQuery q("original_name, fixup", "citation.publisher_fixups");
  if (q.submit(g_server) < 0) {
    citefind::add_to_error_and_exit("unable to get publisher fixups: '" + q.
        error() + "'");
  }
  for (const auto& r : q) {
    g_publisher_fixups.emplace(r[0], r[1]);
  }
}

void fill_doi_list_from_db(DOI_LIST& doi_list) {
  g_output << "    filling list from a database ..." << endl;
  LocalQuery q(g_args.doi_group.doi_query.db_query);
  if (q.submit(g_server) < 0) {
    citefind::add_to_error_and_exit("mysql error '" + q.error() + "' while "
        "getting the DOI list");
  }
  doi_list.reserve(q.num_rows());
  for (const auto& r : q) {
    doi_list.emplace_back(make_tuple(r[0], g_args.doi_group.publisher,
        g_config_data.default_asset_type));
  }
  g_output << "    ... found " << doi_list.size() << " DOIs." << endl;
}

void process_json_value(const JSON::Value& v, deque<string> sp, vector<string>&
    list, string null_value = "") {
  if (v.type() == JSON::ValueType::Array) {
    for (size_t n = 0; n < v.size(); ++n) {
      auto& v2 = v[n];
      process_json_value(v2, sp, list, null_value);
    }
  } else if (v.type() == JSON::ValueType::Object) {
    auto& v2 = v[sp.front()];
    sp.pop_front();
    process_json_value(v2, sp, list, null_value);
  } else if (v.type() == JSON::ValueType::String || v.type() == JSON::
      ValueType::Number) {
    list.emplace_back(v.to_string());
  } else {
    if (v.type() == JSON::ValueType::Null && !null_value.empty()) {
      list.emplace_back(null_value);
    } else {
      citefind::add_to_error_and_exit("json value type '" + to_string(
          static_cast<int>(v.type())) + "' not recognized");
    }
  }
}

void fill_doi_list_from_api(DOI_LIST& doi_list) {
  if (g_args.doi_group.doi_query.api_data.response.doi_path.empty()) {
    citefind::add_to_error_and_exit("not configured to handle an API response");
  }
  g_output << "    filling list from an API ..." << endl;
  size_t num_pages = 1;
  size_t page_num = 1;
  for (size_t n = 0; n < num_pages; ++n) {
    auto url = g_args.doi_group.doi_query.api_data.url;
    if (!g_args.doi_group.doi_query.api_data.pagination.page_num.empty()) {
      if (url.find("?") == string::npos) {
        url += "?";
      } else {
        url += "&";
      }
      url += g_args.doi_group.doi_query.api_data.pagination.page_num + "=" +
          to_string(page_num++);
    }
    stringstream oss, ess;
    if (mysystem2("/bin/tcsh -c \"curl -s -o - '" + url + "'\"", oss, ess) !=
        0) {
      citefind::add_to_error_and_exit("api error '" + ess.str() + "' while "
          "getting the DOI list");
    }
    JSON::Object o(oss.str());
    auto sp = split(g_args.doi_group.doi_query.api_data.response.doi_path, ":");
    auto& v = o[sp.front()];
    sp.pop_front();
    vector<string> dlist;
    process_json_value(v, sp, dlist);
    vector<string> alist;
    if (!g_args.doi_group.doi_query.api_data.response.asset_type_path.empty()) {
      sp = split(g_args.doi_group.doi_query.api_data.response.asset_type_path,
          ":");
      auto& v = o[sp.front()];
      sp.pop_front();
      process_json_value(v, sp, alist, g_config_data.default_asset_type);
      if (alist.size() != dlist.size()) {
        citefind::add_to_error_and_exit("DOI list size != asset type list "
            "size");
      }
    } else {
      alist.insert(alist.begin(), dlist.size(), g_config_data.
          default_asset_type);
    }
    vector<string> plist;
    if (!g_args.doi_group.doi_query.api_data.response.publisher_path.empty()) {
      sp = split(g_args.doi_group.doi_query.api_data.response.publisher_path,
          ":");
      auto& v = o[sp.front()];
      sp.pop_front();
      process_json_value(v, sp, plist, g_args.doi_group.publisher);
      if (plist.size() != dlist.size()) {
        citefind::add_to_error_and_exit("DOI list size != publisher list size");
      }
    } else {
      plist.insert(plist.begin(), dlist.size(), g_args.doi_group.publisher);
    }
    for (size_t m = 0; m < dlist.size(); ++m) {
      doi_list.emplace_back(make_tuple(dlist[m], plist[m], to_lower(alist[m])));
    }
    if (num_pages == 1 && !g_args.doi_group.doi_query.api_data.pagination.
        page_cnt.empty()) {
      auto sp = split(g_args.doi_group.doi_query.api_data.pagination.page_cnt,
          ":");
      auto& v = o[sp.front()];
      sp.pop_front();
      vector<string> list;
      process_json_value(v, sp, list);
      num_pages = stoi(list.front());
    }
  }
  g_output << "    ... found " << doi_list.size() << " DOIs." << endl;
}

void fill_doi_list(DOI_LIST& doi_list) {
  g_output << "Filling list of DOIs for '" << g_args.doi_group.id << "' ..." <<
      endl;
  if (!g_single_doi.empty()) {
    doi_list.emplace_back(make_tuple(g_single_doi, g_args.doi_group.publisher,
        g_config_data.default_asset_type));
  } else if (!g_args.doi_group.doi_query.db_query.empty()) {
    fill_doi_list_from_db(doi_list);
  } else if (!g_args.doi_group.doi_query.api_data.url.empty()) {
    fill_doi_list_from_api(doi_list);
  } else {
    citefind::add_to_error_and_exit("can't figure out how to get the list of "
        "DOIs from the current configuration");
  }
  g_output << "... done filling DOI list." << endl;
}

void query_service(string service_id, const citefind::SERVICE_DATA&
    service_data, const DOI_LIST& doi_list) {
  g_output << "Querying " << get<0>(service_data) << " ..." << endl;
  if (service_id == "crossref") {
    query_crossref(doi_list, service_data);
  } else if (service_id == "elsevier") {
    query_elsevier(doi_list, service_data);
  } else if (service_id == "wos") {
    query_wos(doi_list, service_data);
  } else {
    append(myerror, "ignoring unknown service '" + service_id + "'", "\n");
  }
  g_output << "... done querying " << get<0>(service_data) << "." << endl;
}

void print_publisher_list() {
  LocalQuery q("distinct publisher", "citation.works");
  if (q.submit(g_server) < 0) {
    citefind::add_to_error_and_exit("unable to get list of pubishers from "
        "'works' table: '" + q.error() + "'");
  }
  g_myoutput << "\nCurrent Publisher List:" << endl;
  for (const auto& r : q) {
    g_myoutput << "Publisher: '" << r[0] << "'" << endl;
  }
}

void run_db_integrity_checks() {
}

int main(int argc, char **argv) {
  atexit(citefind::clean_up);
  citefind::read_config();
  clean_cache();
  citefind::parse_args(argc, argv);
  create_doi_table();
  fill_journal_abbreviations();
  fill_journals_no_abbreviation();
  fill_publisher_fixups();
  DOI_LIST doi_list;
  fill_doi_list(doi_list);
  for (const auto& e : g_services) {
    if (get<4>(e.second)) {
      g_mail_message << "Querying '" << e.first << "'." << endl;
      query_service(e.first, e.second, doi_list);
    }
  }
  print_publisher_list();
  run_db_integrity_checks();
  g_server.disconnect();
  g_output.close();
  exit(0);
}
