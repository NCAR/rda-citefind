#include "../../include/citefind.hpp"
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <datetime.hpp>
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
  LocalQuery q("*", "citation.works_authors", "last_name like '%\\\\-%'");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for author last names with escaped "
        "hyphen: '" + q.error() + "'", "\n");
  } else {
    append(myoutput, "  # author last names with an escaped hyphen: " +
        to_string(q.num_rows()), "\n");
  }
  q.set("*", "citation.works_authors", "last_name like '%\\\\''%'");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for author last names with escaped "
        "apostrophe: '" + q.error() + "'", "\n");
  } else {
    append(myoutput, "  # author last names with an escaped apostrophe: " +
        to_string(q.num_rows()), "\n");
  }
}

void check_for_missing_authors() {
  LocalQuery q("select w.doi, count(a.last_name) from citation.works as w left "
      "join citation.works_authors as a on a.id = w.doi group by w.doi having "
      "count(a.last_name) = 0");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for missing authors: '" + q.error() +
        "'", "\n");
  } else {
    append(myoutput, "  # of DOIs with missing authors: " + to_string(q.
        num_rows()), "\n");
    append(myoutput, "    DOI list:", "\n");
    for (const auto& r : q) {
      append(myoutput, "      " + r[0], "\n");
    }
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

void check_for_bad_publication_months() {
  LocalQuery q("*", "citation.works", "pub_month = 0");
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for missing publication months: '" + q.
        error() + "'", "\n");
  } else {
    append(myoutput, "  # works without a publication month: " + to_string(q.
        num_rows()), "\n");
  }
  auto curr_mo = dateutils::current_date_time().to_string("%Y%m");
  q.set("*", "citation.works", "pub_year*100+pub_month > " + curr_mo);
  if (q.submit(g_server) != 0) {
    append(myoutput, "  **Error checking for future publication months: '" + q.
        error() + "'", "\n");
  } else {
    append(myoutput, "  # works with a future publication month: " + to_string(q.
        num_rows()), "\n");
  }
}

void run_db_integrity_checks() {
  check_for_empty_titles();
  check_author_names();
  check_for_missing_authors();
  check_for_bad_publication_months();
  print_publisher_list();
}

} // end namespace citefind
