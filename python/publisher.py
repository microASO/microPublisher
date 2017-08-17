from flask import Flask, request
import os
import logging
import json
import pycurl
import urllib
import dbs.apis.dbsClient as dbsClient
from ServerUtilities import getHashLfn, PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping
from RESTInteractions import HTTPRequests

app = Flask(__name__)

def getProxy(userDN, logger):
    params = {'DN': userDN}
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://asotest3:5000/getproxy'+ '?' + urllib.urlencode(params))
    with open('userProxy', 'w') as f:
        c.setopt(c.WRITEFUNCTION, f.write)
        c.perform()
    c.close()

    return "userProxy"

@app.before_first_request
def setup_logging():
    if not app.debug:
            # In production mode, add log handler to sys.stderr.
            app.logger.addHandler(logging.StreamHandler())
            app.logger.setLevel(logging.INFO)

@app.route('/dbspublish', methods=['POST'])
def publishInDBS3():
    """

    """
    logger = app.logger
    toPublish = request.get_json()
    userDN = request.args.get("DN", "")
    user = request.args.get("User", "")
    workflow = toPublish[0]["taskname"]
    if not workflow:
        logger.info("NO TASKNAME: %s" % toPublish[0])
    for k,v in toPublish[0].iteritems():
        if k == 'taskname':
            logger.info("Starting: %s: %s" % (k, v))
    wfnamemsg = "%s: " % (workflow)

    logger.info(wfnamemsg+" "+user)

    READ_PATH = "/DBSReader"
    READ_PATH_1 = "/DBSReader/"

    opsProxy = '/home/dciangot/proxy'
    proxy = getProxy(userDN, logger)

    oracelInstance = "cmsweb-testbed.cern.ch"
    oracleDB = HTTPRequests(oracelInstance,
                            opsProxy,
                            opsProxy)

    fileDoc = dict()
    fileDoc['subresource'] = 'search'
    fileDoc['workflow'] = workflow

    try:
        results = oracleDB.get('/crabserver/preprod/task',
                                data=encodeRequest(fileDoc))
    except Exception as ex:
        logger.error("Failed to get acquired publications from oracleDB for %s: %s" % (workflow, ex))

    #logger.info(results[0]['desc']['columns'])

    try:
        inputDatasetIndex = results[0]['desc']['columns'].index("tm_input_dataset")
        inputDataset = results[0]['result'][inputDatasetIndex]
        sourceURLIndex = results[0]['desc']['columns'].index("tm_dbs_url")
        sourceURL = results[0]['result'][sourceURLIndex]
        publish_dbs_urlIndex = results[0]['desc']['columns'].index("tm_publish_dbs_url")
        publish_dbs_url = results[0]['result'][publish_dbs_urlIndex]

        #sourceURL = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        if not sourceURL.endswith(READ_PATH) and not sourceURL.endswith(READ_PATH_1):
            sourceURL += READ_PATH


    except Exception:
        logger.exception("ERROR")
    ## When looking up parents may need to look in global DBS as well.
    globalURL = sourceURL
    globalURL = globalURL.replace('phys01', 'global')
    globalURL = globalURL.replace('phys02', 'global')
    globalURL = globalURL.replace('phys03', 'global')
    globalURL = globalURL.replace('caf', 'global')

    pr =  os.environ.get("SOCKS5_PROXY")
    logger.info(wfnamemsg+"Source API URL: %s" % sourceURL)
    sourceApi = dbsClient.DbsApi(url=sourceURL, proxy=pr)
    logger.info(wfnamemsg+"Global API URL: %s" % globalURL)
    globalApi = dbsClient.DbsApi(url=globalURL, proxy=pr)

    WRITE_PATH = "/DBSWriter"
    MIGRATE_PATH = "/DBSMigrate"
    READ_PATH = "/DBSReader"

    if publish_dbs_url.endswith(WRITE_PATH):
        publish_read_url = publish_dbs_url[:-len(WRITE_PATH)] + READ_PATH
        publish_migrate_url = publish_dbs_url[:-len(WRITE_PATH)] + MIGRATE_PATH
    else:
        publish_migrate_url = publish_dbs_url + MIGRATE_PATH
        publish_read_url = publish_dbs_url + READ_PATH
        publish_dbs_url += WRITE_PATH

    logger.debug(wfnamemsg+"Destination API URL: %s" % publish_dbs_url)
    destApi = dbsClient.DbsApi(url=publish_dbs_url, proxy=pr)
    logger.debug(wfnamemsg+"Destination read API URL: %s" % publish_read_url)
    destReadApi = dbsClient.DbsApi(url=publish_read_url, proxy=pr)
    logger.debug(wfnamemsg+"Migration API URL: %s" % publish_migrate_url)
    migrateApi = dbsClient.DbsApi(url=publish_migrate_url, proxy=pr)

    logger.info("inputDataset: %s" % inputDataset)
    noInput = len(inputDataset.split("/")) <= 3
    global_tag = 'crab3_tag'
    primary_ds_type = 'mc'

    # evaluate later
    """
    if not noInput:
        existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
        primary_ds_type = existing_datasets[0]['primary_ds_type']
        # There's little chance this is correct, but it's our best guess for now.
        # CRAB2 uses 'crab2_tag' for all cases
        existing_output = destReadApi.listOutputConfigs(dataset=inputDataset)
        if not existing_output:
            msg = "Unable to list output config for input dataset %s." % (inputDataset)
            logger.error(wfnamemsg+msg)
            global_tag = 'crab3_tag'
        else:
            global_tag = existing_output[0]['global_tag']
    else:
        msg = "This publication appears to be for private MC."
        logger.info(wfnamemsg+msg)
        primary_ds_type = 'mc'
        global_tag = 'crab3_tag'
    """

    acquisition_era_name = "CRAB"
    processing_era_config = {'processing_version': 1, 'description': 'CRAB3_processing_era'}


    logger.info('FINISHED')
    return "FINISHED"   

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=8443,debug=True)
