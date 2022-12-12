from dbs.apis.dbsClient import DbsApi
import ast
import json
import sys
from functools import reduce
from collections import Counter
import operator, itertools
from datetime import datetime
import argparse
import traceback


dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
dbsApi = DbsApi(url = dbsUrl)


"""
This is an example of dbsApi.listFiles
{'adler32': 'adffd927', 'auto_cross_section': 0, 'block_id': 18827000,
'block_name': '/StreamExpress/Run2018A-PromptCalibProdSiStrip-Express-v1/ALCAPROMPT#4b74399f-9f5c-498a-a124-886bef5831d8',
'branch_hash_id': None, 'check_sum': '2633682402', 'create_by': None, 'creation_date': None,
'dataset': '/StreamExpress/Run2018A-PromptCalibProdSiStrip-Express-v1/ALCAPROMPT',
'dataset_id': 13553859, 'event_count': 2042,'file_id': 370276003, 'file_size': 11148903,
'file_type': 'EDM', 'file_type_id': 1, 'is_file_valid': 1, 'last_modification_date': 1527497269,
'last_modified_by': 'tier0@vocms001.cern.ch', 'logical_file_name':
'/store/express/Run2018A/StreamExpress/ALCAPROMPT/PromptCalibProdSiStrip-Express-v1/000/316/995/00000/CCB39171-4E62-E811-B26A-FA163ECEE553.root',
'md5': None, 'run_num': 316995}

This is an example of dbsApi.listFileLumis
[{'logical_file_name': '/store/data/Run2018A/TestEnablesEcalHcal/RAW/Express-v1/000/316/995/00000/D8C818BD-4662-E811-AE4B-FA163ECDD1EA.root',
'run_num': 316995, 'lumi_section_num': [113, 115, 117, 114, 118, 119, 125, 111, 120, 123, 112, 121, 126, 116, 122, 124],
'event_count': [2305, 2317, 2309, 2321, 2321, 2321, 2318, 2312, 2315, 2322, 2309, 2312, 2312, 2314, 2313, 2315]}]
"""

def find_make_and_model(d, lumi, PD):
	lumis=lumi
	pdname=PD
	listfileid=[]
	for k,v in d.items():
		if v["lumi_section_num"] == lumis and v["PDName"] == pdname:
			listfileid.append(v["file_id"])
	return listfileid



def main():
	try:
		parser = argparse.ArgumentParser(description='Check duplicates files after T0 reprocessing. You must source */wmagent/*/init.sh. An example of excecution to check duplicate data would be python3 checkDuplicateFiles.py -RunNumber 316995. Check help section to validate other datatiers and other users and not valid files.')
		parser.add_argument("-RunNumber", "--RunNumber", action="store",type=int, required=True, help="Run Number. Only a single run number is valid for this script")
		parser.add_argument("-DataTier2check", "--DataTier2check", action="store", type=str, required=False, default="RAW", help="Type of DataTier to check. Default datatier is RAW")
		parser.add_argument("-CreatedBy", "--CreatedBy", action="store",type=str, required=False, default="tier0@vocms001.cern.ch", help="User who creates/modify the entry in DBS. Default is Tier0. Use 'all' for all users ")
		parser.add_argument("-ValidFiles", "--ValidFiles", action="store",type=int, required=False, default=1, help="0 is for all files, 1 is for only valid DBS info. Default is 1")
		args = parser.parse_args()
		RunNumber=args.RunNumber
		DataTier=args.DataTier2check
		CreatedBy=args.CreatedBy
		FileisValid=args.ValidFiles

		validDataTier = ["ALCAPROMPT","ALCARECO","AOD","DQMIO","FEVT","MINIAOD","RAW","RAW-RECO"]

		if DataTier not in validDataTier:
			print("DataTier is not valid.")
			return

		print("Getting all the info from DBS")
		listfilesbyRun=dbsApi.listFiles(run_num=RunNumber,validFileOnly=FileisValid,detail=True)
		dictfile={}

		print("Creating a dictionary with the relevant info")

		for fileinfo in listfilesbyRun:
			PD=fileinfo["logical_file_name"].split("/")
			dataTier=PD[5]
			#Check if the file was processed by Tier0
			if dataTier==DataTier and fileinfo["last_modified_by"]==CreatedBy:
				lumifile=dbsApi.listFileLumis(logical_file_name=fileinfo["logical_file_name"])
				#dbs.listFileLumis returns a list, just dealing with dict
				lumifiledict={key:value for e in lumifile for (key,value) in e.items()}
				dictfile[fileinfo["file_id"]]={"nevents":fileinfo["event_count"], "lfn":fileinfo["logical_file_name"], "file_id":fileinfo["file_id"],"PDName":PD[4], "lumi_section_num":lumifiledict["lumi_section_num"], "last_modification_date":datetime.fromtimestamp(fileinfo["last_modification_date"]), "DataTier":PD[5]}
			elif dataTier==DataTier and CreatedBy == "all":
				lumifile=dbsApi.listFileLumis(logical_file_name=fileinfo["logical_file_name"])
				lumifiledict={key:value for e in lumifile for (key,value) in e.items()}
				dictfile[fileinfo["file_id"]]={"nevents":fileinfo["event_count"], "lfn":fileinfo["logical_file_name"], "file_id":fileinfo["file_id"],"PDName":PD[4], "lumi_section_num":lumifiledict["lumi_section_num"], "last_modification_date":datetime.fromtimestamp(fileinfo["last_modification_date"]), "DataTier":PD[5]}

		print("All info contained in dictfile")
		ListData=list(dictfile.values())
		#This will sort the data in function of common Lumis per PD
		print("Sorting the information of common Lumis per PD")
		c={}
		for d in ListData:
			c.setdefault(d['PDName'], []).append(d['lumi_section_num'])

		duplicateDict = [{'PDName': k, 'lumi_section_num': v} for k,v in c.items()]

		for item in duplicateDict:
			print("Checking duplicates for: {}".format(item["PDName"]))
			listLumi=[]
			#print(len(item.get("lumi_section_num")))
			for lumi in item.get("lumi_section_num"):
				#print("{} with lumi {}".format(item["PDName"],lumi))
				fileidsdup=[]
				if lumi in listLumi:
					print("FileDuplicate!!")
					print("{} with lumi {}".format(item["PDName"],lumi))
					fileids=find_make_and_model(dictfile, lumi, item["PDName"])
					fileidsdup.append(fileids)
					for fileid in fileids:
						print(fileid)
						print("------------>",dictfile[fileid])
				else:
					listLumi.append(lumi)



	except Exception:
		print("Hit an exception in checker procedure:")
		traceback.print_exc()

if __name__=="__main__":
	sys.exit(main())
