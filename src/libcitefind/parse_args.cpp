#include "../../include/citefind.hpp"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <strutils.hpp>

using std::cerr;
using std::cout;
using std::endl;
using std::get;
using std::string;
using std::unordered_map;
using strutils::split;
using strutils::trim;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern std::ofstream g_output;
extern unordered_map<string, citefind::SERVICE_DATA> g_services;
extern string g_single_doi;

namespace citefind {

void show_usage_and_exit() {
  cerr << "usage: citefind DOI_GROUP [ options... ]" << endl;
  cerr << "usage: citefind --help" << endl;
  cerr << "usage: citefind --show-doi-groups" << endl;
  cerr << "\nrequired:" << endl;
  cerr << "DOI_GROUP  doi group for which to get citation statistics" << endl;
  cerr << "\noptions:" << endl;
  cerr << "  -d DOI_DATA                get citation data for a single DOI "
      "only (don't" << endl;
  cerr << "                               build a list)" << endl;
  cerr << "                             DOI_DATA is a delimited list (see -s) "
      "containing" << endl;
  cerr << "                               three items:" << endl;
  cerr << "                               - the DOI" << endl;
  cerr << "                               - the publisher of the DOI" << endl;
  cerr << "                               - the asset type (e.g. dataset, "
      "software, etc.)" << endl;
  cerr << "  -k                         keep the json files from the APIs ("
      "default is to" << endl;
  cerr << "                               remove them)" << endl;
  cerr << "  --no-works                 don't collect information about citing "
      "works" << endl;
  cerr << "  --only-services SERVICES   comma-delimited list of services to "
      "query (any not" << endl;
  cerr << "                               specified will be ignored)" << endl;
  cerr << "  --no-services SERVICES     comma-delimited list of services to "
      "ignore" << endl;
  cerr << "  -s DELIMITER               delimiter string for DOI_DATA, "
      "otherwise a" << endl;
  cerr << "                               semicolon is the default" << endl;
  g_args.clean_tmpdir = false;
  exit(1);
}

void show_doi_groups_and_exit() {
  cout << "Known doi groups:" << endl;
  for (const auto& c : g_config_data.doi_groups) {
    cout << "  " << c.id << endl;
  }
  cout << "\nAdd additional doi groups to the configuration file" << endl;
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
  if (a1 == "--show-doi-groups") {
    show_doi_groups_and_exit();
  }
  for (const auto& g : g_config_data.doi_groups) {
    if (g.id == a1) {
      g_args.doi_group = g;
      break;
    }
  }
  if (g_args.doi_group.id.empty()) {
    add_to_error_and_exit("doi group '" + a1 + "' is not configured");
  }
  g_output << "Looking for citation statistics for ID '" << g_args.doi_group.id
      << "'." << endl;
  g_output << "Default publisher is '" << g_args.doi_group.publisher << "'." <<
      endl;
  if (!g_args.doi_group.doi_query.api_data.response.publisher_path.empty()) {
    g_output << "    The path '" << g_args.doi_group.doi_query.api_data.
        response.publisher_path << "' will override the default, if found." <<
        endl;
  }
  if (g_args.doi_group.doi_query.api_data.response.asset_type_path.empty()) {
    g_output << "No asset-type path specified, so using the default value '" <<
        g_config_data.default_asset_type << "' for DOIs." << endl;
  } else {
    g_output << "Specified asset-type path '" << g_args.doi_group.doi_query.
        api_data.response.asset_type_path << "' will be used for DOIs." << endl;
  }
  string doi_data;
  string doi_data_delimiter = ";";
  for (auto n = 2; n < argc; ++n) {
    auto arg = string(argv[n]);
    if (arg == "-d") {
      doi_data = argv[++n];
    } else if (arg == "-k") {
      g_args.clean_tmpdir = false;
    } else if (arg == "--no-works") {
      g_args.no_works = true;
    } else if (arg == "--only-services") {
      auto sp = split(argv[++n], ",");
      for (auto& e : g_services) {
        get<4>(e.second) = false;
      }
      for (const auto& e : sp) {
        if (g_services.find(e) != g_services.end()) {
          get<4>(g_services[e]) = true;
        }
      }
    } else if (arg == "--no-services") {
      auto sp = split(argv[++n], ",");
      for (const auto& e : sp) {
        if (g_services.find(e) != g_services.end()) {
          get<4>(g_services[e]) = false;
        }
      }
    } else if (arg == "-s") {
      doi_data_delimiter = argv[++n];
    }
  }
  if (!doi_data.empty()) {
    auto sp = split(doi_data, doi_data_delimiter);
    if (sp.size() != 3) {
      cerr << "DOI_DATA not specified properly - see usage for details" << endl;
      exit(1);
    }
    g_single_doi = sp[0];
    trim(g_single_doi);
    g_args.doi_group.publisher = sp[1];
    trim(g_args.doi_group.publisher);
    g_config_data.default_asset_type = sp[2];
    trim(g_config_data.default_asset_type);
  }
}

} // end namespace citefind
