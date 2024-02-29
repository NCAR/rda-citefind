#include "../../include/citefind.hpp"
#include <fstream>
#include <thread>
#include <strutils.hpp>
#include <utils.hpp>
#include <json.hpp>

using std::chrono::seconds;
using std::endl;
using std::get;
using std::ifstream;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern std::ofstream g_output;

namespace citefind {

bool filled_authors_from_cross_ref(string subj_doi, JSON::Object& obj) {
  auto cfile = cache_file(subj_doi);
  if (!cfile.empty()) {
    ifstream ifs(cfile.c_str());
    try {
      obj.fill(ifs);
    } catch (...) {
      g_output << "unable to create JSON object from '" << cfile << "'" << endl;
      return false;
    }
    ifs.close();
    if (!obj) {
      g_output << "Error reading CrossRef author JSON for works DOI '" <<
          subj_doi << "': 'unable to create JSON object'" << endl;
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + cfile + "\"", oss, ess);
      return false;
    }
    auto authlst = obj["message"]["author"];
    for (size_t m = 0; m < authlst.size(); ++m) {
      auto family = convert_unicodes(substitute(authlst[m]["family"].
          to_string(), "\\", "\\\\"));
      auto given = convert_unicodes(authlst[m]["given"].to_string());
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
        if (!inserted_works_author(subj_doi, "DOI", fname, mname, family, orcid,
            m, "CrossRef")) {
          stringstream oss, ess;
          mysystem2("/bin/tcsh -c \"/bin/rm -f " + cfile + "\"", oss, ess);
          return false;
        }
      }
    }
  }
  return true;
}

size_t try_crossref(const DOI_DATA& doi_data, const SERVICE_DATA& service_data,
    string& try_error) {
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
        inserted_journal_works_data(sdoi, pubnam, vol, e["page"].to_string(),
            get<0>(service_data));
      } else if (typ == "book-chapter") {
        typ = "C";
        auto isbn = sdoi_obj["message"]["ISBN"][0].to_string();
        if (isbn.empty()) {
          g_output << "Error obtaining CrossRef ISBN for book chapter (DOI: " <<
              sdoi << ")" << endl;
          continue;
        }
        if (!inserted_book_chapter_works_data(sdoi, sdoi_obj["message"]["page"].
            to_string(), isbn, get<0>(service_data))) {
          continue;
        }
        if (!inserted_book_data(isbn, g_config_data.tmpdir)) {
          g_output << "Error inserting ISBN '" << isbn << "' from CrossRef" <<
              endl;
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
        if (!inserted_proceedings_works_data(doi, pubnam, "", "", get<0>(
            service_data))) {
          continue;
        }
      } else {
        g_output << "**UNKNOWN CrossRef TYPE: " << typ << " for work DOI: '" << 
            sdoi << "' citing '" << doi << "'" << endl;
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
      auto ttl = convert_unicodes(repair_string(sdoi_obj["message"]["title"][0].
          to_string()));
      auto publisher = sdoi_obj["message"]["publisher"].to_string();
      if (!inserted_general_works_data(sdoi, ttl, pubyr, typ, publisher, get<0>(
          service_data), "")) {
        continue;
      }
    }
    try_error = "";
    break;
  }
  return ntries;
}

void query_crossref(const DOI_LIST& doi_list, const SERVICE_DATA&
    service_data) {
  string doi, publisher, asset_type;
  for (const auto& e : doi_list) {
    tie(doi, publisher, asset_type) = e;
    g_output << "    querying DOI '" << doi << " | " << publisher << " | " <<
        asset_type << "' ..." << endl;
    string try_error;
    auto ntries = try_crossref(e, service_data, try_error);
    if (ntries == 3) {
      g_output << "Error reading CrossRef JSON for DOI '" << doi << "': '" << 
          try_error << "'\n/bin/tcsh -c \"curl '" << get<2>(service_data)<< 
          "?source=crossref&obj-id=" << doi << "'\"" << endl;
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
      g_output << "unable to create JSON object from '" << filename << "'" <<
          endl;
      return "";
    }
    if (!obj) {
      g_output << "Error reading CrossRef publisher JSON for works DOI '" <<
          subj_doi << "': 'unable to create JSON object'" << endl;
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"/bin/rm -f " + filename + "\"", oss, ess);
      return "";
    }
    return obj["message"]["publisher"].to_string();
  }
  return "";
}

} // end namespace citefind
