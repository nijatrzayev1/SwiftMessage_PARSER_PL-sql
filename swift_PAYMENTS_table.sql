CREATE TABLE swift_PAYMENTS_table
(
  USER_REF_NO             VARCHAR2(16 CHAR),
  BANK_OPER_CODE          VARCHAR2(9 CHAR),
  VALUE_DATE              DATE,
  DR_CCY                  VARCHAR2(3 CHAR),
  DR_AMOUNT               NUMBER(22,2),
  SENDER                  VARCHAR2(50 CHAR),
  BY_ORDER_OF1            VARCHAR2(105 CHAR),
  BY_ORDER_OF2            VARCHAR2(105 CHAR),
  BY_ORDER_OF3            VARCHAR2(105 CHAR),
  BY_ORDER_OF4            VARCHAR2(105 CHAR),
  BY_ORDER_OF5            VARCHAR2(105 CHAR),
  ULTIMATE_BEN1           VARCHAR2(105 CHAR),
  ULTIMATE_BEN2           VARCHAR2(105 CHAR),
  ULTIMATE_BEN3           VARCHAR2(105 CHAR),
  ULTIMATE_BEN4           VARCHAR2(105 CHAR),
  ULTIMATE_BEN5           VARCHAR2(105 CHAR),
  PAYMENT_DETAILS1        VARCHAR2(105 CHAR),
  PAYMENT_DETAILS2        VARCHAR2(105 CHAR),
  PAYMENT_DETAILS3        VARCHAR2(105 CHAR),
  PAYMENT_DETAILS4        VARCHAR2(105 CHAR),
  INFORMATION1            VARCHAR2(105 CHAR),
  INFORMATION2            VARCHAR2(105 CHAR),
  INFORMATION3            VARCHAR2(105 CHAR),
  INFORMATION4            VARCHAR2(105 CHAR),
  INFORMATION5            VARCHAR2(105 CHAR),
  MSG_DATETIME            DATE                  DEFAULT sysdate,
  FILE_ID                 NUMBER,
  PACKAGE_REF_NO          VARCHAR2(50 CHAR),
  SENDER53                VARCHAR2(30 CHAR),
  SENDER54                VARCHAR2(30 CHAR),
)