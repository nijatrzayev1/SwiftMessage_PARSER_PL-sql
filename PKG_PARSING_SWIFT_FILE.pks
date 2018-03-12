CREATE OR REPLACE PACKAGE pkg_parsing_swift_file is

  -- Author  : Nijat M. Rzayev
  -- Created : 03-10-2017 14:42:01
  -- Purpose : SWIFT AZIPS


  c_directory constant varchar2(50) := '/u01/swiftparser';
  c_directory1 constant varchar2(50) := 'SWFILE_DIR';

  TYPE swift_type IS TABLE OF varchar2(32767) INDEX BY VARCHAR2(64);

  type FTTBS_UPLOAD is record(
    FT_CONTRACT_REF  VARCHAR2(16),
    USER_REF_NO      VARCHAR2(16),
    bank_oper_code   VARCHAR2(9),
    value_date       DATE,
    dr_ccy           VARCHAR2(3),
    dr_amount        NUMBER(22,2),
    sender           varchar2(50),
    by_order_of1     VARCHAR2(105),
    by_order_of2     VARCHAR2(105),
    by_order_of3     VARCHAR2(105),
    by_order_of4     VARCHAR2(105),
    by_order_of5     VARCHAR2(105),
    ultimate_ben1    VARCHAR2(105),
    ultimate_ben2    VARCHAR2(105),
    ultimate_ben3    VARCHAR2(105),
    ultimate_ben4    VARCHAR2(105),
    ultimate_ben5    VARCHAR2(105),
    payment_details1 VARCHAR2(105),
    payment_details2 VARCHAR2(105),
    payment_details3 VARCHAR2(105),
    payment_details4 VARCHAR2(105),
    INFORMATION1     VARCHAR2(105),
    INFORMATION2     VARCHAR2(105),
    INFORMATION3     VARCHAR2(105),
    INFORMATION4     VARCHAR2(105),
    INFORMATION5     VARCHAR2(105),
    PACKAGE_REF_NO   varchar2(50),
    SENDER53         varchar2(50),
    SENDER54         varchar2(50));

  type t_FTTBS_UPLOAD is table of FTTBS_UPLOAD;
  t_swift_type swift_type;

  procedure load_file;
  procedure processing_parsing_file;
  procedure set_msg_type;
end pkg_parsing_swift_file;

/