#include "../../include/citefind.hpp"
#include <fstream>
#include <sys/stat.h>
#include <thread>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <json.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::chrono::seconds;
using std::endl;
using std::ifstream;
using std::this_thread::sleep_for;
using std::string;
using std::stringstream;
using std::to_string;
using strutils::append;
using strutils::split;
using strutils::sql_ready;
using strutils::substitute;
using unixutils::mysystem2;

extern citefind::Args g_args;
extern Server g_server;
extern std::ofstream g_output;

namespace citefind {

bool inserted_works_author(string pid, string pid_type, string first_name,
    string middle_name, string last_name, string orcid_id, size_t sequence,
    string whence) {
  string columns = "id, id_type, last_name, first_name, middle_name, sequence";
  string values = "'" + pid + "', '" + pid_type + "', '" + sql_ready(last_name)
      + "', '" + sql_ready(first_name) + "', '" + sql_ready(middle_name) + "', "
      + to_string(sequence);
  if (!orcid_id.empty()) {
    columns += ", orcid_id";
    values += ", '" + orcid_id + "'";
  }
  string on_conflict = "";
  if (whence == "Open Library" || whence == "CrossRef") {
    on_conflict = "on constraint works_authors_pkey do update set last_name = "
        "case when length(excluded.last_name) > length(works_authors."
        "last_name) then excluded.last_name else works_authors.last_name end, "
        "first_name = case when length(excluded.first_name) > length("
        "works_authors.first_name) then excluded.first_name else works_authors."
        "first_name end, middle_name = case when length(excluded.middle_name) "
        "> length(works_authors.middle_name) then excluded.middle_name else "
        "works_authors.middle_name end";
    if (!orcid_id.empty()) {
      on_conflict += ", orcid_id = case when excluded.orcid_id is not null "
          "then excluded.orcid_id else works_authors.orcid_id end";
    }
  }
  auto status = g_server.insert(
      "citation.works_authors",
      columns,
      values,
      on_conflict
  );
  if (status != 0) {
    if (status == -2) {
      LocalQuery q("last_name, first_name, middle_name, orcid_id", "citation."
          "works_authors", "id = '" + pid + "' and id_type = '" + pid_type +
          "and sequence = " + to_string(sequence));
      if (q.submit(g_server) != 0) {
        append(myerror, "Query error on duplicate author check: '" + q.error() +
            "'", "\n");
      } else {
         Row row;
         if (!q.fetch_row(row)) {
           append(myerror, "Row fetch error on duplicate author check: '" + q.
               error() + "'", "\n");
         } else {
           if (row[0] != last_name || row[1] != first_name || row[2] !=
               middle_name || row[3] != orcid_id) {
             g_output << "-##-DUPLICATE AUTHOR MISMATCH (" << whence << "): "
                 << pid_type << ": " << pid << ", last=('" << row[0] << "','" <<
                 last_name << "'), first=('" << row[1] << "','" << first_name <<
                 "'), middle=('" << row[2] << "','" << middle_name << "'), "
                 "orcid_id=('" << row[3] << "','" << orcid_id << "'), sequence="
                 << to_string(sequence) << endl;
           }
         }
      }
    } else {
      g_output << "Error while inserting author (" << pid << "," << pid_type << 
          "," << last_name << "," << first_name << "," << middle_name << "," << 
          to_string(sequence) << "," << orcid_id << "): '" << g_server.error()
          << "' from " << whence << endl;
      return false;
    }
  }
  return true;
}

bool inserted_general_works_data(string doi, string title, string pub_year,
    string works_type, string publisher, string service, string service_id) {
  if (g_server.insert(
        "citation.works",
        "doi, title, pub_year, type, publisher",
        "'" + doi + "', '" + sql_ready(title) + "', '" + pub_year + "', '" +
            works_type + "', '" + sql_ready(publisher) + "'",
        "on constraint works_pkey do update set title = case when length("
            "excluded.title) > length(works.title) then excluded.title "
            "else works.title end, publisher = case when length(excluded."
            "publisher) > length(works.publisher) then excluded.publisher "
            "else works.publisher end"
        ) < 0) {
    g_output << "Error while inserting " << service << "(" << service_id << ") "
        "work (" << doi << "," << title << "," << pub_year << "," << works_type
        << "," << publisher << "): '" << g_server.error() << "'" << endl;
    return false;
  }
  return true;
}

bool inserted_book_works_data(string isbn, string title, string publisher,
    string service) {
  if (g_server.insert(
        "citation.book_works",
        "isbn, title, publisher",
        "'" + isbn + "', '" + sql_ready(title) + "', '" + publisher + "'",
        "on constraint book_works_pkey do update set title = case when length("
            "excluded.title) > length(book_works.title) then excluded.title "
            "else book_works.title end, publisher = case when length(excluded."
            "publisher) > length(book_works.publisher) then excluded.publisher "
            "else book_works.publisher end"
        ) < 0) {
    g_output << "Error while inserting " << service << " book data (" << isbn <<
        "," << title << "," << publisher << "): '" << g_server.error() << "'" <<
        endl;
    return false;
  }
  return true;
}

bool inserted_book_chapter_works_data(string doi, string pages, string isbn,
    string service) {
  if (g_server.insert(
        "citation.book_chapter_works",
        "doi, pages, isbn",
        "'" + doi + "', '" + pages + "', '" + isbn + "'",
        "on constraint book_chapter_works_pkey do update set pages = "
            "case when length(excluded.pages) > length("
            "book_chapter_works.pages) then excluded.pages else "
            "book_chapter_works.pages end, isbn = case when length("
            "excluded.isbn) > length(book_chapter_works.isbn) then "
            "excluded.isbn else book_chapter_works.isbn end"
        ) < 0) {
    g_output << "Error while inserting " << service << " book chapter data (" <<
        doi << "," << pages << "," << isbn << "): '" << g_server.error() << "'"
        << endl;
    return false;
  }
  return true;
}

bool inserted_journal_works_data(string doi, string pub_name, string volume,
    string pages, string service) {
  if (g_server.insert(
        "citation.journal_works",
        "doi, pub_name, volume, pages",
        "'" + doi + "', '" + sql_ready(pub_name) + "', '" + volume + "', '" +
            pages + "'",
        "on constraint journal_works_pkey do update set pub_name = case "
            "when length(excluded.pub_name) > length(journal_works."
            "pub_name) then excluded.pub_name else journal_works."
            "pub_name end, volume = case when length(excluded.volume) > "
            "length(journal_works.volume) then excluded.volume else "
            "journal_works.volume end, pages = case when length(excluded."
            "pages) > length(journal_works.pages) then excluded.pages "
            "else journal_works.pages end"
        ) < 0) {
    g_output << "Error while inserting " << service << " journal data (" << doi
        << "," << pub_name << "," << volume << "," << pages << "): '" <<
        g_server.error() << "'" << endl;
    return false;
  }
  return true;
}

bool inserted_proceedings_works_data(string doi, string pub_name, string volume,
    string pages, string service) {
  if (g_server.insert(
        "citation.proceedings_works",
        "doi, pub_name, volume, pages",
        "'" + doi + "', '" + sql_ready(pub_name) + "', '" + volume + "', '" +
            pages + "'",
        "on constraint proceedings_works_pkey do update set pub_name = "
            "case when length(excluded.pub_name) > length("
            "proceedings_works.pub_name) then excluded.pub_name else "
            "proceedings_works.pub_name end, volume = case when length("
            "excluded.volume) > length(proceedings_works.volume) then "
            "excluded.volume else proceedings_works.volume end, pages = "
            "case when length(excluded.pages) > length(proceedings_works."
            "pages) then excluded.pages else proceedings_works.pages end"
        ) < 0) {
    g_output << "Error while inserting " << service << " proceedings data (" <<
        doi << ", " << pub_name << ", " << volume << ", " << pages << "): '" <<
        g_server.error() << "'" << endl;
    return false;
  }
  return true;
}

bool inserted_citation(string doi, string doi_work, string service) {
  if (g_server.insert(
        g_args.doi_group.insert_table,
        "doi_data, doi_work, new_flag",
        "'" + doi + "', '" + doi_work + "', '1'",
        "(doi_data, doi_work) do nothing"
        ) < 0) {
    g_output << "Error while inserting " << service << " citation (" << doi <<
        ", " << doi_work << "): '" << g_server.error() << "'" << endl;
    return false;
  }
  return true;
}

void insert_source(string doi_work, string doi_data, string service) {
  if (g_server.insert(
      "citation.sources",
      "doi_work, doi_data, source",
      "'" + doi_work + "', '" + doi_data + "', '" + service + "'",
      "on constraint sources_pkey do nothing"
      ) < 0) {
    g_output << "Error updating " << service << " source for '" << doi_work <<
        "', '" << doi_data << "'" << endl;
  }
}

bool inserted_doi_data(string doi, string publisher, string asset_type, string
    service) {
  if (g_server.insert(
        "citation.doi_data",
        "doi_data, publisher, asset_type",
        "'" + doi + "', '" + sql_ready(publisher) + "', '" + asset_type + "'",
        "on constraint doi_data_pkey do update set publisher = case when "
            "length(excluded.publisher) > length(doi_data.publisher) then "
            "excluded.publisher else doi_data.publisher end, asset_type = case "
            "when length(excluded.asset_type) > length(doi_data.asset_type) "
            "then excluded.asset_type else doi_data.asset_type end"
        ) < 0) {
    g_output << "Error updating " << service << " DOI data (" << doi << ", " <<
        publisher << ", " << asset_type << "): '" << g_server.error() << "'" <<
        endl;
    return false;
  }
  return true;
}

bool inserted_book_data_from_google(string isbn, string tmpdir) {
  auto fn_isbn = tmpdir + "/cache/" + isbn + ".google.json";
  struct stat buf;
  if (stat(fn_isbn.c_str(), &buf) != 0) {
    sleep_for(seconds(1));
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"curl -s -o " + fn_isbn + " 'https://"
        "www.googleapis.com/books/v1/volumes?q=isbn:" + isbn + "'\"", oss, ess);
    if (!ess.str().empty()) {
      g_output << "Error retrieving book data from Google for ISBN '" << isbn <<
          "'" << endl;
      return false;
    }
  }
  ifstream ifs(fn_isbn.c_str());
  auto e = myerror;
  JSON::Object isbn_obj(ifs);
  if (!isbn_obj) {
    g_output << "Error reading Google JSON for ISBN '" << isbn << "' : '" <<
        myerror << "'" << endl;
    stringstream oss, ess;
    mysystem2("/bin/rm " + fn_isbn, oss, ess);
    return false;
  }
  myerror = e;
  if (isbn_obj["items"][0]["volumeInfo"]["authors"].size() == 0) {
    g_output << "Empty Google data for ISBN : '" << isbn << "'" << endl;
    return false;
  }
  for (size_t m = 0; m < isbn_obj["items"][0]["volumeInfo"]["authors"].size();
      ++m) {
    auto sp = split(isbn_obj["items"][0]["volumeInfo"]["authors"][m].
        to_string());
    auto fnam = sp.front();
    string mnam;
    size_t next = 1;
    if (sp.size() > 2) {
      mnam = sp[next++];
    }
    string lnam;
    for (size_t n = next; n < sp.size(); ++n) {
      append(lnam, sp[n], " ");
    }
    if (!inserted_works_author(isbn, "ISBN", fnam, mnam, lnam, "", m, "Google "
        "Books")) {
      return false;
    }
  }
  auto ttl = substitute(isbn_obj["items"][0]["volumeInfo"]["title"].to_string(),
      "\\", "\\\\");
  return inserted_book_works_data(isbn, ttl, isbn_obj["items"][0]["volumeInfo"][
      "publisher"].to_string(), "Google Books");
}

bool inserted_book_data_from_openlibrary(string isbn, string tmpdir) {
  auto fn_isbn = tmpdir + "/cache/" + isbn + ".openlibrary.json";
  struct stat buf;
  if (stat(fn_isbn.c_str(), &buf) != 0) {
    sleep_for(seconds(1));
    stringstream oss, ess;
    mysystem2("/bin/tcsh -c \"curl -s -o " + fn_isbn + " 'https://"
        "openlibrary.org/api/books?bibkeys=ISBN:" + isbn + "&jscmd="
        "details&format=json'\"", oss, ess);
    if (!ess.str().empty()) {
      g_output << "Error retrieving book data from Open Library for ISBN '" <<
          isbn << "'" << endl;
      return false;
    }
  }
  ifstream ifs(fn_isbn.c_str());
  auto e = myerror;
  JSON::Object isbn_obj(ifs);
  if (!isbn_obj) {
    stringstream oss, ess;
    mysystem2("/bin/rm " + fn_isbn, oss, ess);
    g_output << "Error reading Open Library JSON for ISBN '" << isbn << "' : '"
        << myerror << "'" << endl;
    return false;
  }
  myerror = e;
  auto o = isbn_obj["ISBN:" + isbn]["details"];
  auto oa = o["authors"];
  if (oa.size() == 0) {
    g_output << "Empty Open Library data for ISBN : '" << isbn << "'" << endl;
    return false;
  }
  for (size_t m = 0; m < oa.size(); ++m) {
    auto sp = split(oa[m]["name"].to_string());
    auto fnam = sp.front();
    string mnam;
    size_t next = 1;
    if (sp.size() > 2) {
      mnam = sp[next++];
    }
    string lnam;
    for (size_t n = next; n < sp.size(); ++n) {
      append(lnam, sp[n], " ");
    }
    if (!inserted_works_author(isbn, "ISBN", fnam, mnam, lnam, "", m, "Open "
        "Library")) {
      return false;
    }
  }
  auto ttl = substitute(o["title"].to_string(), "\\", "\\\\");
  return inserted_book_works_data(isbn, ttl, o["publishers"][0].to_string(),
      "Open Library");
}

bool inserted_book_data(string isbn, string tmpdir) {
  auto b = inserted_book_data_from_google(isbn, tmpdir);
  if (!b) {
    b = inserted_book_data_from_openlibrary(isbn, tmpdir);
  }
  return b;
}

} // end namespace citefind
