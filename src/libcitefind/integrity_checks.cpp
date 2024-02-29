#include "../../include/citefind.hpp"
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::endl;
using std::stringstream;
using strutils::append;

extern Server g_server;

namespace citefind {

void print_publisher_list() {
  LocalQuery q("distinct publisher", "citation.works");
  if (q.submit(g_server) < 0) {
    add_to_error_and_exit("unable to get list of pubishers from 'works' table: "
    "'" + q.error() + "'");
  }
  append(myoutput, "Current Publisher List:", "\n");
  for (const auto& r : q) {
    append(myoutput, "Publisher: '" + r[0] + "'", "\n");
  }
}

void run_db_integrity_checks() {
  print_publisher_list();
}

} // end namespace citefind
