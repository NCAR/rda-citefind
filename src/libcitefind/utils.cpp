#include "../../include/citefind.hpp"
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using std::endl;
using std::string;
using std::stringstream;
using strutils::append;
using strutils::replace_all;
using unixutils::mysystem2;

extern citefind::ConfigData g_config_data;
extern citefind::Args g_args;
extern stringstream g_myoutput, g_mail_message;

namespace citefind {

void clean_up() {
  if (g_args.clean_tmpdir) {
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"/bin/rm -f " + g_config_data.tmpdir + "/*.json\"",
        oss, ess);
  }
  if (!myerror.empty()) {
    g_mail_message << myerror << endl;
  }
  if (!g_myoutput.str().empty()) {
    g_mail_message << g_myoutput.str() << endl;
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
  replace_all(value, "\\u00a0", " ");
  replace_all(value, "\\u2010", "-");
  replace_all(value, "\\u2013", "-");
  replace_all(value, "\\u2014", "-");
  replace_all(value, "\\u2019", "'");
  return value;
}

} // end namespace citefind
