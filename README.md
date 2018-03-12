# SwiftMessage_PARSER_PL-sql
# Author: Nijat M. Rzayev
1. Create attached packages and config tables from script.
2. Insert Swift message body into table.
3.Run script.
Done!

Script firstly find message type (102,103,950 or ect.) from Clob file.
From config table system looking tag names and length of line.
Then insert into data table.
After insert you can create transaction using any webservice or method.
