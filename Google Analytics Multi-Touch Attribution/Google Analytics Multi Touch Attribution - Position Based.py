"""
	Christian Kinley
	4/3/2019
	
	Resources:
	https://developers.google.com/analytics/devguides/reporting/mcf/v3/reference
	https://developers.google.com/resources/api-libraries/documentation/analytics/v3/python/latest/analytics_v3.data.mcf.html"""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import json
from urllib.error import HTTPError
import urllib.parse as urlparse
import csv
import requests
import pyodbc
import pandas as pd
import collections
from decimal import *
import argparse
import os
from datetime import datetime, timedelta

#Variables for the from_json_keyfile_name function's parameter values.
key_file_location = 'keyfile.json'
scope = 'https://www.googleapis.com/auth/analytics.readonly'
	
#Variables for the build() function's parameter values.
api_name = 'analytics'
api_version = 'v3'

def get_command_line_arguments():
	"""Get the arguments supplied by the user via the command line and return a dictionary containing the arguments.
	
	Args:
		None.
	Returns:
		A dictionary where each key represents the argument name and the value represents the argument value.
	"""
	
	yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
	
	parser = argparse.ArgumentParser(description='GA Position-Based Attribution')
	
	parser.add_argument('viewFile', type=str, default='Views.txt', nargs='?', help='Filepath for a text file containing a newline-separated list of GA view IDs in the format url;viewID')
	parser.add_argument('startDate', type=str, default = yesterday, nargs='?', help='Start date for the data you want in the format yyyy-mm-dd')
	parser.add_argument('endDate', type=str, default = yesterday, nargs='?', help='End date for the data you want in the format yyyy-mm-dd')
	
	args = parser.parse_args()
	
	argDictionary = {'viewFile': args.viewFile, 'startDate': args.startDate, 'endDate': args.endDate}
	
	return argDictionary

def get_service():
	"""Initialize a Google Analytics API service object.

	Returns:
		An authorized Google Analytics API service object.
	"""
	
	credentials = ServiceAccountCredentials.from_json_keyfile_name(
		key_file_location, scopes=scope)
		
	#Build the service object.
	service = build(serviceName=api_name, version=api_version, credentials=credentials)
	
	return service

def etl_for_each_view(service, viewFile, startDate, endDate):
	"""For each view in the view list and each date in the date range, extract interaction data from 
	   the Multi Channel Funnels Reporting API, transform it for our needs, and load it into the 
	   SQL Server database table.
	
	Args:
		service: The authorized Google Analytics API service object.
		viewFile: Filepath for a text file containing a newline-separated list of GA view IDs in the format url;viewID.
		startDate: The beginning date of the range for which to pull data from the API.
		endDate: The end date of the range for which to pull data from the API.
	Returns:
		Nothing.
	"""
	
	#Create a list from viewFile containing the views
	viewFile = open(viewFile, 'r')
	viewText = viewFile.read()
	viewList = viewText.split('\n')
	
	finalViewList = []
	
	#Separate the URLs from the view IDs to create a list of lists, where the inner lists are URL/view ID pairs
	for view in viewList:
		finalViewList.append(view.split(';'))
	
	#Variable to hold the desired date range
	daterange = pd.date_range(startDate, endDate)
	
	#For each view in the view list...
	for view in finalViewList:
		#...and for each date in the date range, perform ETL of attribution data
		for date in daterange:
			response = get_report(service=service, viewId='ga:'+view[1], startDate=date.strftime("%Y-%m-%d"), endDate=date.strftime("%Y-%m-%d"))
			# # write_to_json(response)
			# response = read_from_json()
			#Ensure that we have data in the API response for this website and date before trying to use it. Not all websites have a transaction every day.
			if 'rows' in response[0]:
				table = create_table_from_response(response=response)
				transactionDictionary = apply_position_based_attribution_model(table)
				finalTable = prepare_data_for_insert(transactionDictionary=transactionDictionary, url=view[0], date=date.strftime("%Y-%m-%d")) #The URL & date will be appended to each row
				insert_data_into_database(finalTable)
	
def get_start_index(next_link):
	"""Grab the start-index query parameter from the nextLink property of the API response. This is used to grab additional response pages.
	
	Args:
		next_link: The value of the nextLink property in the API response.
	Returns:
		The value of the start_index query parameter from the nextLink property.
	"""
		
	next_link = urlparse.parse_qs(urlparse.urlparse(next_link).query) #Grab the query parameter from the nextLink URL.
	
	if next_link.get('start-index') is None: #None object indicates that there are no additional pages.
		return next_link.get('start-index')
	else:
		return int(next_link.get('start-index')[0])
	
def get_report(service, viewId, startDate, endDate):		
	"""Query the Multi Channel Funnels Reporting API.

	Args:
		service: The authorized Google Analytics API service object.
		viewId: The view ID of the Google Analytics view for which to pull data from the API.
		startDate: The beginning date of the range for which to pull data from the API.
		endDate: The end date of the range for which to pull data from the API.
	Returns:
		The Multi Channel Funnels Reporting API response.
	"""
	
	#Variables used in get_report function.
	response = []
	response_page = 0

	#There will always be at least one response page. This block is for the first response page.
	response.append(service.data().mcf().get(
		ids = viewId, #View ID.
		start_date = startDate, #Start date.
		end_date = endDate, #End date.
		metrics = 'mcf:totalConversionValue', #Metrics
		dimensions = 'mcf:sourcePath,mcf:mediumPath,mcf:campaignPath,mcf:transactionId',
		filters = 'mcf:transactionId!=(not set)',
		max_results = 10000 #The maximum number of rows permitted by the MCF Reporting API.
		).execute()) #Dimensions.
	start_index = get_start_index(response[response_page].get('nextLink')) #Grab the start_index of the next page of the response (if present).
	total_results = response[response_page].get('totalResults') #Grab the total number of rows in the query result so that we can know how many pages we need to get.
	
	#If there are additional response pages, this block appends them to the response variable.
	while start_index is not None: #None object indicates that there are no additional pages.
		response.append(service.data().mcf().get(
			ids = viewId, #View ID.
			start_date = startDate, #Start date.
			end_date = endDate, #End date.
			metrics = 'mcf:totalConversionValue', #Metrics
			dimensions = 'mcf:sourcePath,mcf:mediumPath,mcf:campaignPath,mcf:transactionId',
			filters = 'mcf:transactionId!=(not set)',
			max_results = 10000, #The maximum number of rows permitted by the MCF Reporting API.
			start_index = start_index
			).execute()) #Dimensions.
		response_page += 1 #The index of the most recent response page in the response list.
		start_index = get_start_index(response[response_page].get('nextLink')) #Grab the start_index of the next page of the response (if present).
	
	return response
	
def create_table_from_response(response):
	"""Create a table (list of lists) from the Multi Channel Funnels Reporting API response data.

	Args:
		response: The Multi Channel Funnels Reporting API response in the form of a JSON file.
	Returns:
		table: A two-dimensional list representing rows of data.
	"""
	
	#A list to hold the rows data.
	table = []

	#A list to temporarily hold the content of each row so it can be appended to table.
	tempList = []
	sourcePath = ''
	mediumPath = ''
	campaignPath = ''
	
	for i in range(0, len(response)):
		for row in response[i].get('rows'): #Can potentially be hardcoded - there should be only one "rows" object per dictionary.
			#Grab the source path for the row
			for j in row[0].get('conversionPathValue'):
				sourcePath = sourcePath + j.get('nodeValue') + ' > '
			
			sourcePath = sourcePath.rstrip(' > ')
			tempList.append(sourcePath)
			sourcePath = ''
			
			#Grab the medium path for the row
			for k in row[1].get('conversionPathValue'):
				mediumPath = mediumPath + k.get('nodeValue') + ' > '
			
			mediumPath = mediumPath.rstrip(' > ')
			tempList.append(mediumPath)
			mediumPath = ''
			
			#Grab the campaign path for the row
			for l in row[2].get('conversionPathValue'):
				campaignPath = campaignPath + l.get('nodeValue') + ' > '
			
			campaignPath = campaignPath.rstrip(' > ')
			tempList.append(campaignPath)
			campaignPath = ''
			
			#Grab the total conversion value for the row
			transactionID = row[3].get('primitiveValue')
			tempList.append(transactionID)
			
			#Append the tempList (representing the current row of data) to the table
			table.append(tempList)
			tempList = []

	return table
	
def apply_position_based_attribution_model(table):
	"""Apply a position-based attribution model to the input table.

	Args:
		table: A two-dimensional list, where each row represents the interaction path for one transaction.
		       Source Path | Medium Path | Campaign Path | Transaction ID
	Returns:
		transactionDictionary: A dictionary where the keys are TransactionIDs, whose values are dictionaries where the keys are 
		                       Source/Medium/Campaign sets and the values are the proportion of revenue credit that set gets. 
	"""
	
	#A dictionary to hold the credit proportion for each TransactionID-Source/Medium/Campaign set.
	transactionDictionary = {}

	#Proportion of conversion credit to give each interaction position
	firstInteractionCredit = Decimal(0.30)
	middleInteractionCredit = Decimal(0.30)
	lastInteractionCredit = Decimal(0.40)
	
	#Delimiter for Source, Medium, & Campaign
	delimiter = '/'
	
	#List to store each Source, Medium, & Campaign set temporarily for each row.
	smcSets = []
	
	#Table to store the rows after concatenating Source, Medium, & Campaign, and doing direct swapping.
	refinedTable = []
	
	#Convert the table to a DataFrame
	df = pd.DataFrame(table, columns=['SourcePath','MediumPath','CampaignPath','TransactionID'])
	
	#Replace all "/" characters with empty string. "/" will be used as the delimiter for sourceMediumCampaignPath.
	df = df.replace(to_replace='/', value='')

	#Iterate through each row of df in order to concatenate the Source, Medium, & Campaign of each path.
	for row in df.itertuples(index=False):
		sourcePathList = row.SourcePath.split(' > ')
		mediumPathList = row.MediumPath.split(' > ')
		campaignPathList = row.CampaignPath.split(' > ')
		
		#For the current row, create an ordered list of each Source-Medium-Campaign set. The result is a list of tuples, where each tuple is a Source-Medium-Campaign set.
		sourceMediumCampaignList = list(zip(sourcePathList, mediumPathList, campaignPathList))
		
		#For each Source-Medium-Campaign set in the current row, concatenate the Source, Medium, & Campaign using "/" as the delimiter.
		for tuple in sourceMediumCampaignList:
			sourceMediumCampaign = delimiter.join(tuple)
			smcSets.append(sourceMediumCampaign)
		
		#If '(direct)/(none)/(unavailable)' exists in the current smcSets AND a non '(direct)/(none)/(unavailable)' set exists in the current smcSets, then remove all '(direct)/(none)/(unavailable)' sets. Otherwise, leave '(direct)/(none)/(unavailable)' in scmSets (since it is the only set in the conversion path).
		tempSmcSets = [x for x in smcSets if x != '(direct)/(none)/(unavailable)']
		
		if len(tempSmcSets) > 0:
			smcSets = tempSmcSets
		
		#Append the TransactionID for the current row to the end of the current Source, Medium, Campaign set list, append current row to refinedTable, then reset smcSets.
		smcSets.append(row.TransactionID)
		refinedTable.append(smcSets)
		smcSets = []
		tempSmcSets = []
	
	#Iterate through refinedTable, recording the proportion of credit that each Source/Medium/Campaign-Transaction ID set gets.
	for row in refinedTable:
			
		#If there is only one Source/Medium/Campaign set for the row, then give it all of the credit.
		if len(row) == 2:
			if row[1] not in transactionDictionary: #We will only record the first instance of a TransactionID to help eliminate duplicates.
				transactionDictionary[row[1]] = {row[0]: 1.00}
		#If there are only two Source/Medium/Campaign sets for the row, then assign the usual first & last credit, then divide the remaining middle credit according to the ratio of first:last.
		elif len(row) == 3:
			if row[2] not in transactionDictionary: #We will only record the first instance of a TransactionID to help eliminate duplicates.
				tempDictionary = {} #tempDictionary will store the value of the transactionDictionary key.
				
				#Assign credit to first interaction
				tempDictionary[row[0]] = firstInteractionCredit + (firstInteractionCredit / (firstInteractionCredit + lastInteractionCredit) * middleInteractionCredit)
				
				#Assign credit to second (last) interaction
				if row[1] in tempDictionary:
					tempDictionary[row[1]] += lastInteractionCredit + (lastInteractionCredit / (firstInteractionCredit + lastInteractionCredit) * middleInteractionCredit) #In case the first & second (last) interactions have the same Source/Medium/Campaign.
				else:
					tempDictionary[row[1]] = lastInteractionCredit + (lastInteractionCredit / (firstInteractionCredit + lastInteractionCredit) * middleInteractionCredit)
						
				transactionDictionary[row[2]] = tempDictionary
		#If there are more than three Source/Medium/Campaign sets for the row, then assign credit to first & last interaction, then loop through middle interactions and assign credit to each.
		else:
			if row[-1] not in transactionDictionary:
				tempDictionary = {} #tempDictionary will store the value of the transactionDictionary key.
					
				#Assign credit to first interaction.
				tempDictionary[row[0]] = firstInteractionCredit
					
				#Assign credit to last interaction.
				if row[-2] in tempDictionary:
					tempDictionary[row[-2]] += lastInteractionCredit #In case the first & last interactions have the same Source/Medium/Campaign.
				else:
					tempDictionary[row[-2]] = lastInteractionCredit
					
				#Assign credit for middle interactions.
				countOfMiddleInteractions = len(row[1:-2])
				for middleInteraction in row[1:-2]:
					if middleInteraction in tempDictionary:
						tempDictionary[middleInteraction] += middleInteractionCredit / countOfMiddleInteractions
					else:
						tempDictionary[middleInteraction] = middleInteractionCredit / countOfMiddleInteractions
					
				transactionDictionary[row[-1]] = tempDictionary
	
	# #Round the credit proportions to 5 decimal places.
	for key, value in transactionDictionary.items():
		for innerKey, innerValue in value.items():
			transactionDictionary[key][innerKey] = round(innerValue, 5)

	return transactionDictionary
	
def prepare_data_for_insert(transactionDictionary, url, date):
	"""Writes the data from the medium dictionary into a two-dimensional list so that it can be inserted into the database.

	Args:
		transactionDictionary: A dictionary where the keys are TransactionIDs, whose values are dictionaries where the keys are 
		                       Source/Medium/Campaign sets and the values are the proportion of revenue credit that set gets.
		url: The URL of the site for which data has been pulled from the API.
		date: The date for which data has been pulled from the API.
	Returns:
		finalTable: A two-dimensional list representing rows of data.
	"""
	
	#A list to hold the medium attribution data.
	finalTable = []
	
	#A list to temporarily hold the content of each row so it can be appended to table.
	tempList = []
	
	#Construct a table (2-dimensional list) where each row is: GADate | Website | TransactionID | Source | Medium | Campaign | CreditProportion
	for key, value in transactionDictionary.items():
		for innerKey, innerValue in value.items():
			smcList = innerKey.split('/')
			tempList.extend((date, url, key, smcList[0], smcList[1], smcList[2], innerValue))
			finalTable.append(tempList)
			tempList = []
	
	return finalTable
	
def insert_data_into_database(finalTable):
	"""Insert the data into the database.

	Args:
		finalTable: A two-dimensional list representing rows of data.
	"""
	
	#Write table to database
	conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=server_name;DATABASE=database_name;Trusted_Connection=yes;')

	cursor = conn.cursor()

	cursor.executemany('INSERT INTO Table(GADate, Website, TransactionID, Source, Medium, Campaign, RevenueProportion) VALUES (?, ?, ?, ?, ?, ?, ?)', finalTable)

	conn.commit()
	
def main():
	argDictionary = get_command_line_arguments()
	service = get_service()
	etl_for_each_view(service=service, viewFile=argDictionary.get('viewFile'), startDate=argDictionary.get('startDate'), endDate=argDictionary.get('endDate'))
	
if __name__ == '__main__':
	main()
