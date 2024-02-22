#ifndef CITEFIND_H
#define  CITEFIND_H

#include <string>
#include <vector>

namespace citefind {

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
extern void clean_up();
extern void read_config();

extern std::string convert_unicodes(std::string value);

} // end namespace citefind

#endif
