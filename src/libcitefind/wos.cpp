#include "../../include/citefind.hpp"
#include <fstream>
#include <sys/stat.h>
#include <strutils.hpp>
#include <utils.hpp>

using std::endl;
using std::get;
using std::ifstream;
using std::string;
using std::stringstream;
using std::to_string;
using std::tuple;
using std::vector;
using strutils::append;
using strutils::split;
using strutils::to_lower;
using strutils::to_title;
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern std::ofstream g_output;

namespace citefind {

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
  if (v.type() == JSON::ValueType::Nonexistent) {
    return;
  }
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
    inserted_works_author(doi_work, "DOI", get<0>(name), get<1>(name), get<2>(
        name), get<3>(name), get<4>(name), "WoS");
  }
}

void query_wos(const DOI_LIST& doi_list, const SERVICE_DATA& service_data) {
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
    if (mysystem2("/bin/tcsh -c \"curl -H '" + API_KEY_HEADER + "' '" + API_URL
        + "/?databaseId=DCI&usrQuery=DO=" + doi + "&count=1&firstRecord=1&"
        "viewField=none'\"", oss, ess) < 0) {
      g_output << "Error getting WoS ID for DOI '" << doi << "'" << endl;
      continue;
    }
    JSON::Object o(oss.str());
    if (!o) {
      g_output << "Wos response for DOI '" << doi << "' ID is not JSON" << endl;
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
        g_output << "Error getting WoS citation IDs for DOI '" << doi << "'" <<
            endl;
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
          g_output << "Error get WoS work data for ID '" << work_id << "' (DOI "
              << doi << ")" << endl;
          continue;
        }
      }
      ifstream ifs(cache_fn.c_str());
      o.fill(ifs);
      if (!o) {
        g_output << "Error - '" << cache_fn << "' is not a JSON file (DOI " << 
            doi << ")" << endl;
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
        g_output << "Missing work DOI for WoS ID '" << work_id << "' (DOI " << 
            doi << ")" << endl;
        continue;
      }
      g_output << "        WoS ID: '" << work_id << "', DOI: '" << doi_work <<
          "'" << endl;
      if (!inserted_citation(doi, doi_work, get<0>(service_data), g_args.
          doi_group.insert_table)) {
        continue;
      }
      insert_source(doi_work, doi, get<0>(service_data));
      if (!inserted_doi_data(doi, publisher, asset_type, get<0>(
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
          g_output << "Unable to find WoS publication name (" << work_id << ")"
              << endl;
          continue;
        }
        auto vol = pub_info["vol"].to_string();
        auto issue = pub_info["issue"].to_string();
        if (!issue.empty()) {
          vol += "(" + issue + ")";
        }
        if (!inserted_journal_works_data(doi_work, pubnam, vol, pub_info[
            "page"]["content"].to_string(), get<0>(service_data))) {
          continue;
        }
      } else {
        g_output << "Unknown WoS pubtype '" << pubtype << "' (" << work_id << 
            ")" << endl;
        continue;
      }
      if (item_title.empty()) {
        g_output << "Unable to find WoS item title (" << work_id << ")" << endl;
        continue;
      }

      // add general data about the "work"
      auto pubyr = pub_info["pubyear"].to_string();
      if (!pubyr.empty()) {
        string pubmo = "0";
        auto sd = pub_info["sortdate"].to_string();
        if (!sd.empty()) {
          auto sp = split(sd, "-");
          if (sp.size() == 3) {
            pubmo = sp[1];
            if (pubmo[0] == '0') {
              pubmo.erase(0, 1);
            }
          }
        }
        auto& p = summary["publishers"]["publisher"]["names"]["name"];
        auto publisher = p["unified_name"].to_string();
        if (publisher.empty()) {
          publisher = to_title(p["full_name"].to_string());
        }
        if (publisher.empty()) {
          g_output << "**NO WoS PUBLISHER (" << work_id << ")" << endl;
          continue;
        }
        if (!inserted_general_works_data(doi_work, item_title, pubyr, pubmo,
            pubtype, publisher, get<0>(service_data), work_id)) {
          continue;
        }
      } else {
        g_output << "**NO WoS PUBLICATION YEAR (" << work_id << ")" << endl;
      }
    }
  }
  if (g_args.doi_group.id == "rda") {
    regenerate_dataset_descriptions(get<0>(service_data));
  }
  reset_new_flag();
}

} // end namespace citefind
