#!/usr/bin/env bash
#
# run application
#

# run the server
umask 0002
cd bin; ./dpg-jobs-ws              \
  -archive    ${DPG_ARCHIVE_PATH}  \
  -delivery   ${DPG_DELIVERY_PATH} \
  -iiif       ${DPG_IIIF_PATH}     \
  -iiifman    ${IIIF_MAN}          \
  -work       ${DPG_WORK_PATH}     \
  -dbhost     ${DBHOST}            \
  -dbport     ${DBPORT}            \
  -dbname     ${DBNAME}            \
  -dbuser     ${DBUSER}            \
  -dbpass     ${DBPASS}            \
  -smtphost   ${SMPT_HOST}         \
  -smtpport   ${SMPT_PORT}         \
  -smtpuser   ${SMPT_USER}         \
  -smtppass   ${SMPT_PASS}         \
  -smtpsender ${SMPT_SENDER}       \
  -tsapi      ${TSAPI}             \
  -tsadmin    ${TSADMIN}           \
  -reindex    ${REINDEX}

# return the status
exit $?

#
# end of file
#
