#include "../../include/citefind.hpp"
#include <fstream>
#include <sys/stat.h>
#include <regex>
#include <thread>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

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
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern std::ofstream g_output;
extern unordered_map<string, string> g_publisher_fixups;

namespace citefind {

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
        if (!inserted_citation(doi, sdoi, get<0>(service_data))) {
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
            append(myerror, "Error obtaining Elsevier ISBN for book chapter "
                "(DOI: " + sdoi + ")", "\n");
            continue;
          }
          if (!inserted_book_chapter_works_data(sdoi, doi_obj["search-results"][
              "entry"][n]["prism:pageRange"].to_string(),
              isbn, get<0>(service_data))) {
            continue;
          }
          if (!inserted_book_data(isbn)) {
            append(myerror, "Error inserting ISBN '" + isbn + "' from Elsevier",
                "\n");
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
          if (!inserted_general_works_data(sdoi, ttl, pubyr, typ, publisher,
              get<0>(service_data), scopus_url)) {
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

} // end namespace citefind
