#!/usr/bin/env bash
#
# run application
#

# run the server
umask 0002
cd bin; ./dpg-jobs-ws               \
  -archive    ${DPG_ARCHIVE_PATH}   \
  -delivery   ${DPG_DELIVERY_PATH}  \
  -iiifman    ${IIIF_MAN}           \
  -iiifstage  ${IIIF_STAGE_DIR}     \
  -iiifbucket ${IIIF_AWS_BUCKET}    \
  -work       ${DPG_WORK_PATH}      \
  -dbhost     ${DBHOST}             \
  -dbport     ${DBPORT}             \
  -dbname     ${DBNAME}             \
  -dbuser     ${DBUSER}             \
  -dbpass     ${DBPASS}             \
  -smtphost   ${SMPT_HOST}          \
  -smtpport   ${SMPT_PORT}          \
  -smtpuser   ${SMPT_USER}          \
  -smtppass   ${SMPT_PASS}          \
  -smtpsender ${SMPT_SENDER}        \
  -asuser     ${AS_USER}            \
  -aspass     ${AS_PASS}            \
  -htftps     ${HT_FTPS}            \
  -htuser     ${HT_USER}            \
  -htpass     ${HT_PASS}            \
  -rcbin      ${RC_BIN}             \
  -rccfg      ${RC_CFG}             \
  -rcremote   ${RC_REMOTE}          \
  -rcdir      ${RC_DIR}             \
  -tsapi      ${TSAPI}              \
  -tsadmin    ${TSADMIN}            \
  -reindex    ${REINDEX}            \
  -xmlreindex ${XML_REINDEX}        \
  -aptbucket  ${APTRUST_AWS_BUCKET} \
  -apthost    ${APTRUST_AWS_HOST}   \
  -ocr        ${DPG_OCR}            \
  -pdf        ${DPG_PDF}            \
  -service    ${DPG_SERVICE_URL}

# return the status
exit $?

#
# end of file
#
