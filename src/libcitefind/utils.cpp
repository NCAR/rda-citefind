#include "../../include/citefind.hpp"
#include <fstream>
#include <thread>
#include <sys/stat.h>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::chrono::seconds;
using std::endl;
using std::ifstream;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::trim;
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern Server g_server;
extern stringstream g_mail_message;
extern std::ofstream g_output;

namespace citefind {

void clean_up() {
  if (g_args.clean_tmpdir) {
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"/bin/rm -f " + g_config_data.tmpdir + "/*.json\"",
        oss, ess);
  }
  if (!myerror.empty()) {
    g_output << myerror << endl;
  }
  if (!myoutput.empty()) {
    g_mail_message << myoutput << endl;
  }
  if (!g_mail_message.str().empty()) {
    unixutils::sendmail("dattore@ucar.edu", "dattore@ucar.edu", "", "citefind "
        "cron for " + g_args.doi_group.id, g_mail_message.str());
  }
}

void add_to_error_and_exit(string msg) {
  append(myerror, msg, "\n");
  exit(1);
}

string convert_unicodes(string value) {
  replace_all(value, "\\\\u00a0", " ");
  replace_all(value, "\\u00a0", " ");
  replace_all(value, "\\\\u2010", "-");
  replace_all(value, "\\u2010", "-");
  replace_all(value, "\\\\u2013", "-");
  replace_all(value, "\\u2013", "-");
  replace_all(value, "\\\\u2014", "-");
  replace_all(value, "\\u2014", "-");
  replace_all(value, "\\\\u2019", "'");
  replace_all(value, "\\u2019", "'");
  return value;
}

void reset_new_flag() {
  if (g_server.command("update " + g_args.doi_group.insert_table + " set "
      "new_flag = '0' where new_flag = '1'") < 0) {
    append(myerror, "Error updating 'new_flag' in " + g_args.doi_group.
        insert_table + ": " + g_server.error(), "\n");
    return;
  }
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

void clean_cache() {
  stringstream oss, ess;
  if (mysystem2("/bin/tcsh -c 'find " + g_config_data.tmpdir + "/cache/* "
      "-mtime +180 -exec rm {} \\;'", oss, ess) != 0) {
    add_to_error_and_exit("unable to clean cache - error: '" + ess.str() + "'");
  }
}

} // end namespace citefind
