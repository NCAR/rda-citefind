#include "../../include/citefind.hpp"
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::endl;
using std::stringstream;
using std::to_string;
using strutils::append;

extern Server g_server;

namespace citefind {

void check_for_empty_titles() {
  LocalQuery q("*", "citation.works", "title = ''");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for empty titles: '" + q.error() +
        "'", "\n");
  } else {
    append(myoutput, "  # works without a title: " + to_string(q.num_rows()),
        "\n");
  }
}

void check_author_names() {
  LocalQuery q("*", "citation.works_authors", "last_name like '%\\-%'");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for author last names with escaped "
        "hyphen: '" + q.error() + "'", "\n");
  } else {
    append(myoutput, "  # author last names with an escaped hyphen: " +
        to_string(q.num_rows()), "\n");
  }
  q.set("*", "citation.works_authors", "last_name like '%\\'%'");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for author last names with escaped "
        "apostrophe: '" + q.error() + "'", "\n");
  } else {
    append(myoutput, "  # author last names with an escaped apostrophe: " +
        to_string(q.num_rows()), "\n");
  }
}

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
  check_for_empty_titles();
  check_author_names();
  print_publisher_list();
}

} // end namespace citefind
