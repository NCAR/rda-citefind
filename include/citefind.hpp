#ifndef CITEFIND_H
#define  CITEFIND_H

#include <string>
#include <vector>
#include <json.hpp>

namespace citefind {

typedef std::tuple<std::string, std::string, std::string> DOI_DATA;
typedef std::vector<citefind::DOI_DATA> DOI_LIST;
typedef std::tuple<std::string, std::string, std::string, std::string, bool>
    SERVICE_DATA;

struct ConfigData {
  struct DOI_Group {
    DOI_Group() : id(), publisher(), insert_table(), doi_query() { }

    struct API_Data {
      struct Response {
        Response() : doi_path(), publisher_path(), asset_type_path() { }

        std::string doi_path, publisher_path, asset_type_path;
      };

      struct Pagination {
        Pagination() : page_num(), page_cnt() { }

        std::string page_num, page_cnt;
      };

      API_Data() : url(), response(), pagination() { }

      std::string url;
      Response response;
      Pagination pagination;
    };

    struct DOI_Query {
      DOI_Query() : db_query(), api_data() { }

      std::string db_query;
      API_Data api_data;
    };

    std::string id, publisher, insert_table;
    DOI_Query doi_query;
  };

  ConfigData() : tmpdir(), default_asset_type(), doi_groups() { }

  std::string tmpdir, default_asset_type;
  std::vector<DOI_Group> doi_groups;
};

struct Args {
  Args() : doi_group(), clean_tmpdir(true), no_works(false) { }

  ConfigData::DOI_Group doi_group;
  bool clean_tmpdir, no_works;
};

extern void add_to_error_and_exit(std::string msg);
extern void clean_cache();
extern void clean_up();
extern void fill_doi_list(DOI_LIST& doi_list);
extern void get_citations(std::string url, std::string service_id, size_t sleep,
    std::string filename, JSON::Object& doi_obj);
extern void insert_source(std::string doi_work, std::string doi_data, std::
    string service);
extern void parse_args(int argc, char **argv);
extern void query_crossref(const DOI_LIST& doi_list, const SERVICE_DATA&
    service_data);
extern void query_elsevier(const DOI_LIST& doi_list, const SERVICE_DATA&
    service_data);
extern void query_wos(const DOI_LIST& doi_list, const SERVICE_DATA&
    service_data);
extern void read_config();
extern void regenerate_dataset_descriptions(std::string whence);
extern void reset_new_flag();

extern std::string cache_file(std::string doi);
extern std::string convert_unicodes(std::string value);
extern std::string publisher_from_cross_ref(std::string subj_doi);
extern std::string repair_string(std::string s);

extern bool filled_authors_from_cross_ref(std::string subj_doi, JSON::Object&
    obj);
extern bool inserted_book_data(std::string isbn);
extern bool inserted_book_data_from_google(std::string isbn);
extern bool inserted_book_data_from_openlibrary(std::string isbn);
extern bool inserted_book_works_data(std::string isbn, std::string title, std::
    string publisher, std::string service);
extern bool inserted_book_chapter_works_data(std::string doi, std::string pages,
    std::string isbn, std::string service);
extern bool inserted_citation(std::string doi, std::string doi_work, std::string
    service);
extern bool inserted_doi_data(std::string doi, std::string publisher, std::
    string asset_type, std::string service);
extern bool inserted_general_works_data(std::string doi, std::string title,
    std::string pub_year, std::string works_type, std::string publisher, std::
    string service, std::string service_id);
extern bool inserted_journal_works_data(std::string doi, std::string pub_name,
    std::string volume, std::string pages, std::string service);
extern bool inserted_proceedings_works_data(std::string doi, std::string
    pub_name, std::string volume, std::string pages, std::string service);
extern bool inserted_works_author(std::string pid, std::string pid_type, std::
    string first_name, std::string middle_name, std::string last_name, std::
    string orcid_id, size_t sequence, std::string whence);

} // end namespace citefind

#endif
