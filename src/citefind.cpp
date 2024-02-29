#include "../include/citefind.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <sys/stat.h>
#include <PostgreSQL.hpp>
#include <json.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tempfile.hpp>

using namespace PostgreSQL;
using std::endl;
using std::find;
using std::get;
using std::stoi;
using std::string;
using std::stringstream;
using std::to_string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::split;
using unixutils::mysystem2;

string myerror = "";
string mywarning = "";
string myoutput = "";
citefind::ConfigData g_config_data;
citefind::Args g_args;
Server g_server;
stringstream g_mail_message;
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

int main(int argc, char **argv) {
  atexit(citefind::clean_up);
  citefind::read_config();
  citefind::clean_cache();
  citefind::parse_args(argc, argv);
  create_doi_table();
//  fill_journal_abbreviations();
//  fill_journals_no_abbreviation();
  citefind::DOI_LIST doi_list;
  citefind::fill_doi_list(doi_list);
  for (const auto& e : g_services) {
    if (get<4>(e.second)) {
      g_mail_message << "Querying '" << e.first << "'." << endl;
      query_service(e.first, e.second, doi_list);
    }
  }
  citefind::run_db_integrity_checks();
  g_server.disconnect();
  g_output.close();
  exit(0);
}
