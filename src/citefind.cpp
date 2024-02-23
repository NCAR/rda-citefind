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
//unordered_map<string, string> g_journal_abbreviations;
unordered_map<string, string> g_publisher_fixups;
//unordered_set<string> g_journals_no_abbreviation;
std::ofstream g_output;
string g_single_doi;

unordered_map<string, citefind::SERVICE_DATA> g_services;

/*
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
*/

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

/*
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
*/

/*
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
*/

void fill_doi_list_from_db(citefind::DOI_LIST& doi_list) {
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

void fill_doi_list_from_api(citefind::DOI_LIST& doi_list) {
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

void fill_doi_list(citefind::DOI_LIST& doi_list) {
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
    service_data, const citefind::DOI_LIST& doi_list) {
  g_output << "Querying " << get<0>(service_data) << " ..." << endl;
  if (service_id == "crossref") {
    citefind::query_crossref(doi_list, service_data);
  } else if (service_id == "elsevier") {
    citefind::query_elsevier(doi_list, service_data);
  } else if (service_id == "wos") {
    citefind::query_wos(doi_list, service_data);
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
//  fill_journal_abbreviations();
//  fill_journals_no_abbreviation();
  citefind::DOI_LIST doi_list;
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
