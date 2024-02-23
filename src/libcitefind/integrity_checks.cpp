#include "../../include/citefind.hpp"
#include <PostgreSQL.hpp>

using namespace PostgreSQL;
using std::endl;
using std::stringstream;

extern Server g_server;
extern stringstream g_myoutput;

namespace citefind {

void print_publisher_list() {
  LocalQuery q("distinct publisher", "citation.works");
  if (q.submit(g_server) < 0) {
    add_to_error_and_exit("unable to get list of pubishers from 'works' table: "
    "'" + q.error() + "'");
  }
  g_myoutput << "\nCurrent Publisher List:" << endl;
  for (const auto& r : q) {
    g_myoutput << "Publisher: '" << r[0] << "'" << endl;
  }
}

void run_db_integrity_checks() {
  print_publisher_list();
}

} // end namespace citefind
