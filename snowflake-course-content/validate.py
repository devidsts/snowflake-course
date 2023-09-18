#!/usr/bin/env python
import snowflake.connector
# Gets the version
ctx = snowflake.connector.connect(
 user='<user_name>',
 password='<password>',
 account='<account_identifier>'
 )
cs = ctx.cursor()
try:
 cs.execute("SELECT current_version()")
 one_row = cs.fetchone()
 print(one_row[0])
finally:
 cs.close()
ctx.close()