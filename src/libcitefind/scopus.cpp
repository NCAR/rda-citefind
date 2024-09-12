#include "../../include/citefind.hpp"
#include <fstream>
#include <sys/stat.h>
#include <regex>
#include <thread>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <datetime.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::chrono::seconds;
using std::endl;
using std::get;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using std::to_string;
using std::unordered_map;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::trim;
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern Server g_server;
extern std::ofstream g_output;

namespace citefind {

void fill_publisher_fixups(unordered_map<string, string>& publisher_fixups) {
  LocalQuery q("original_name, fixup", "citation.publisher_fixups");
  if (q.submit(g_server) < 0) {
    add_to_error_and_exit("unable to get publisher fixups: '" + q.error() +
        "'");
  }
  for (const auto& r : q) {
    publisher_fixups.emplace(r[0], r[1]);
  }
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
      g_output << "Error while getting Elsevier author data for scopus ID '" <<
          scopus_id << "': '" << ess.str() << "'" << endl;
      return false;
    }
  }
  std::ifstream ifs(authfil.c_str());
  try {
    author_obj.fill(ifs);
  } catch(...) {
    g_output << "unable to create JSON object from '" << authfil << "'" << endl;
    return false;
  }
  ifs.close();
  if (!author_obj) {
    g_output << "Error reading Elsevier JSON for scopus id '" << scopus_id<< 
        "': 'unable to create JSON object'\n/bin/tcsh -c \"curl -o " << authfil
        << " '" << url << "'\"" << endl;
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
      if (!inserted_works_author(subj_doi, "DOI", fnam, mnam, lnam, "", stoi(
          seq), "Scopus")) {
        return false;
      }
    }
    return true;
  }
  return false;
}

void query_elsevier(const DOI_LIST& doi_list, const SERVICE_DATA&
    service_data) {
  const regex email_re("(.)*@(.)*\\.(.)*");
  unordered_map<string, string> publisher_fixups;
  fill_publisher_fixups(publisher_fixups);
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
        g_output << "Error reading Elsevier JSON for DOI '" << doi << "': '"
            "unable to create JSON object'\n/bin/tcsh -c \"curl -o " << filename
            << " '" << url << "'\"" << endl;
        continue;
      }
      try {
        totres = stoi(doi_obj["search-results"]["opensearch:totalResults"].
            to_string());
      } catch (const std::invalid_argument& e) {
        if (doi_obj["error-response"]["error-code"].to_string() ==
            "TOO_MANY_REQUESTS") {
          g_output << "... exiting Scopus query due to rate limiting" << endl;
          append(myoutput, "*** exiting Scopus query due to rate limiting",
              "\n");
          stringstream oss, ess;
          mysystem2("/bin/tcsh -c \"curl -s -I '" + url + "' |grep "
              "'^x-ratelimit-reset' |awk -F: '{print $2}'\"", oss, ess);
          if (!oss.str().empty()) {
            auto dt = DateTime(1970, 1, 1, 0, 0).seconds_added(stoi(oss.str()));
            append(myoutput, " - rate limiting resets at " + dt.to_string());
          }
          return;
        }
        g_output << "invalid JSON for '" << url << "'" << endl;
        continue;
      } catch (...) {
        g_output << "unknown error in JSON for '" << url << "'" << endl;
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
          g_output << "**NO Elsevier WORKS DOI:  Data DOI - " << doi<< 
              "  type - " << doi_obj["search-results"]["entry"][n][
              "prism:aggregationType"].to_string() << "  title - '" << doi_obj[
              "search-results"]["entry"][n]["dc:title"].to_string() << "'" <<
              endl;
          continue;
        }
        auto sdoi = doi_obj["search-results"]["entry"][n]["prism:doi"].
            to_string();
        replace_all(sdoi, "\\/","/");
        if (!inserted_citation(doi, sdoi, get<0>(service_data), g_args.
            doi_group.insert_table)) {
          continue;
        }
        insert_source(sdoi, doi, get<0>(service_data));
        if (!inserted_doi_data(doi, publisher, asset_type, get<0>(
            service_data))) {
          continue;
        }
        if (g_args.no_works) {
          continue;
        }

        // add the author data for the citing "work"
        auto prism_url = doi_obj["search-results"]["entry"][n]["prism:url"];
        if (prism_url.type() == JSON::ValueType::Nonexistent) {
          g_output << "**NO Elsevier SCOPUS ID: " << filename << " " << 
              prism_url.to_string() << endl;
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
          if (!inserted_journal_works_data(sdoi, pubnam, doi_obj[
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
            g_output << "Error obtaining Elsevier ISBN for book chapter (DOI: "
                << sdoi << ")" << endl;
            continue;
          } else if (isbn.front() == '[' && isbn.back() == ']') {
            isbn.pop_back();
            auto sp = split(isbn.substr(1), ",");
            isbn = sp.front();
            trim(isbn);
          }
          if (!inserted_book_chapter_works_data(sdoi, doi_obj["search-results"][
              "entry"][n]["prism:pageRange"].to_string(),
              isbn, get<0>(service_data))) {
            continue;
          }
          if (!inserted_book_data(isbn, g_config_data.tmpdir)) {
            g_output << "Error inserting ISBN '" << isbn << "' from Elsevier" <<
                endl;
            continue;
          }
        } else if (typ == "Conference Proceeding") {
          typ = "P";
          auto& e = doi_obj["search-results"]["entry"][n];
          if (!inserted_proceedings_works_data(sdoi, e["prism:publicationName"].
              to_string(), e["prism:volume"].to_string(), e["prism:pageRange"].
              to_string(), get<0>(service_data))) {
            continue;
          }
        } else {
          g_output << "**UNKNOWN Elsevier TYPE: " << typ << " for work DOI: '"
              << sdoi << "' citing '" << doi << "'" << endl;
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
          if (publisher_fixups.find(publisher) != publisher_fixups.end()) {
            publisher = publisher_fixups[publisher];
          } else {
            if (regex_search(publisher, email_re)) {
              if (publisher_fixups.find(publisher) == publisher_fixups.end()) {
                g_output << "**SUSPECT PUBLISHER: '" << publisher << "'" <<
                    endl;
              } else {
                publisher = publisher_fixups[publisher];
              }
            }
          }
          if (!inserted_general_works_data(sdoi, ttl, pubyr, typ, publisher,
              get<0>(service_data), scopus_url)) {
            continue;
          }
        } else {
          g_output << "**NO Elsevier PUBLICATION YEAR: SCOPUS URL - " << 
              scopus_url << endl;
        }
      }
    }
  }
  if (g_args.doi_group.id == "rda") {
    regenerate_dataset_descriptions(get<0>(service_data));
  }
  reset_new_flag();
}

} // end namespace citefind
