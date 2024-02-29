#include "../../include/citefind.hpp"
#include <fstream>
#include <sys/stat.h>
#include <unordered_map>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <json.hpp>
#include <datetime.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::endl;
using std::string;
using std::stringstream;
using std::to_string;
using std::unordered_map;
using strutils::append;
using strutils::replace_all;

extern Server g_server;
extern citefind::ConfigData g_config_data;
extern std::ofstream g_output;
extern unordered_map<string, citefind::SERVICE_DATA> g_services;

namespace citefind {

string url_encode(string url) {
  replace_all(url, "[", "%5B");
  replace_all(url, "]", "%5D");
  return url;
}

void assert_configuration_value(string value_name, const JSON::Value& value,
    JSON::ValueType assert_type, string id) {
  if (value.type() != assert_type) {
    add_to_error_and_exit("'" + value_name + "' not found in configuration "
        "file, or is not a string (id=" + id + ")");
  }
}

void read_config() {
  std::ifstream ifs("/glade/u/home/dattore/citefind/conf/local_citefind.conf");
  if (!ifs.is_open()) {
    add_to_error_and_exit("unable to open configuration file");
  }
  JSON::Object o(ifs);
  ifs.close();
  assert_configuration_value("temp-dir", o["temp-dir"], JSON::ValueType::String,
      "main");
  g_config_data.tmpdir = o["temp-dir"].to_string();
  assert_configuration_value("default-asset-type", o["default-asset-type"],
      JSON::ValueType::String, "main");
  g_config_data.default_asset_type = o["default-asset-type"].to_string();
  struct stat buf;
  if (stat(g_config_data.tmpdir.c_str(), &buf) != 0) {
    add_to_error_and_exit("temporary directory '" + g_config_data.tmpdir +
        "' is missing");
  }
  if (o["db-config"].type() == JSON::ValueType::Nonexistent) {
    add_to_error_and_exit("no database configuration found");
  }
  g_server.connect(o["db-config"]["host"].to_string(), o["db-config"][
      "username"].to_string(), o["db-config"]["password"].to_string(), o[
      "db-config"]["schema"].to_string());
  if (!g_server) {
    add_to_error_and_exit("unable to connect to the database");
  }
  auto output_name = g_config_data.tmpdir + "/output." + dateutils::
      current_date_time().to_string("%Y%m%d%H%MM");
  g_output.open(output_name);
  g_output << "Configuration file open and ready to parse ..." << endl;
  append(myoutput, "Output is in '" + output_name + "'", "\n");
  auto& doi_groups = o["doi-groups"];
  for (size_t n = 0; n < doi_groups.size(); ++n) {
    auto& grp = doi_groups[n];
    g_config_data.doi_groups.emplace_back(ConfigData::DOI_Group());
    auto& c = g_config_data.doi_groups.back();
    assert_configuration_value("id", grp["id"], JSON::ValueType::String,
        to_string(n));
    c.id = grp["id"].to_string();
    g_output << "    found ID '" << grp["id"].to_string() << "'" << endl;
    assert_configuration_value("publisher", grp["publisher"], JSON::ValueType::
        String, c.id);
    c.publisher = grp["publisher"].to_string();
    assert_configuration_value("db-insert-table", grp["db-insert-table"], JSON::
        ValueType::String, c.id);
    c.insert_table = grp["db-insert-table"].to_string();
    assert_configuration_value("doi-query", grp["doi-query"], JSON::ValueType::
        Object, c.id);
    auto& q = grp["doi-query"];
    if (q["db-query"].type() == JSON::ValueType::String) {
      c.doi_query.db_query = q["db-query"].to_string();
    } else {
      assert_configuration_value("api-query", q["api-query"], JSON::ValueType::
          Object, c.id);
      auto& a = q["api-query"];
      assert_configuration_value("url", a["url"], JSON::ValueType::String, c.
          id);
      c.doi_query.api_data.url = a["url"].to_string();
      assert_configuration_value("response:doi", a["response"]["doi"], JSON::
          ValueType::String, c.id);
      c.doi_query.api_data.response.doi_path = a["response"]["doi"].to_string();
      assert_configuration_value("response:publisher", a["response"][
          "publisher"], JSON::ValueType::String, c.id);
      c.doi_query.api_data.response.publisher_path = a["response"]["publisher"].
          to_string();
      if (a["response"]["asset-type"].type() == JSON::ValueType::String) {
        c.doi_query.api_data.response.asset_type_path = a["response"][
            "asset-type"].to_string();
      }
      if (a["pagination"].type() != JSON::ValueType::Nonexistent) {
        auto& paging = a["pagination"];
        assert_configuration_value("pagination:page-count", paging[
            "page-count"], JSON::ValueType::String, c.id);
        c.doi_query.api_data.pagination.page_cnt = url_encode(paging[
            "page-count"].to_string());
        assert_configuration_value("pagination:page-number", paging[
            "page-number"], JSON::ValueType::String, c.id);
        c.doi_query.api_data.pagination.page_num = url_encode(paging[
            "page-number"].to_string());
      }
    }
  }
  auto& services = o["services"];
  for (size_t n = 0; n < services.size(); ++n) {
    auto id = services[n]["id"].to_string();
    if (!id.empty()) {
      g_services.emplace(id, make_tuple(services[n]["name"].to_string(),
          services[n]["title"].to_string(), services[n]["url"].to_string(),
          services[n]["api-key"].to_string(), true));
    }
  }
  g_output << "... configuration file parsed." << endl;
}

} // end namespace citefind
