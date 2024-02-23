#include "../../include/citefind.hpp"
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <json.hpp>

using namespace PostgreSQL;
using std::deque;
using std::endl;
using std::make_tuple;
using std::string;
using std::stringstream;
using std::stoi;
using std::to_string;
using std::tuple;
using std::vector;
using strutils::split;
using strutils::to_lower;
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern Server g_server;
extern std::ofstream g_output;
extern string g_single_doi;

namespace citefind {

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

void fill_doi_list_from_db(DOI_LIST& doi_list) {
  g_output << "    filling list from a database ..." << endl;
  LocalQuery q(g_args.doi_group.doi_query.db_query);
  if (q.submit(g_server) < 0) {
    add_to_error_and_exit("mysql error '" + q.error() + "' while getting the "
        "DOI list");
  }
  doi_list.reserve(q.num_rows());
  for (const auto& r : q) {
    doi_list.emplace_back(make_tuple(r[0], g_args.doi_group.publisher,
        g_config_data.default_asset_type));
  }
  g_output << "    ... found " << doi_list.size() << " DOIs." << endl;
}

void fill_doi_list_from_api(DOI_LIST& doi_list) {
  if (g_args.doi_group.doi_query.api_data.response.doi_path.empty()) {
    add_to_error_and_exit("not configured to handle an API response");
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
      add_to_error_and_exit("api error '" + ess.str() + "' while getting the "
          "DOI list");
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
        add_to_error_and_exit("DOI list size != asset type list size");
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
        add_to_error_and_exit("DOI list size != publisher list size");
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
    add_to_error_and_exit("can't figure out how to get the list of DOIs from "
        "the current configuration");
  }
  g_output << "... done filling DOI list." << endl;
}

} // end namespace citefind
