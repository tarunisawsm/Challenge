#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <curl/curl.h>
#include <json/json.h>
#include <sqlite3.h>
#include <string.h>
#include "task_defs.h"

//Callback function to handle the SQLite database call
static int callback( void *NotUsed, int argc, char **argv, char **azColName )
{
	int i;
	for(i=0; i<argc; i++)
	{
		printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");
	return 0;
}

void downloadData( const char *url, char *fileName )
{
    CURL *handle;
    handle = curl_easy_init();
	bool downloadFlag = false;
    if( handle )
    {
        curl_easy_setopt( handle, CURLOPT_URL, url ) ;
        FILE* file = fopen( fileName, "w");
        if( NULL == file )
		{
			printf("Error opening file\n");
		}
		else
		{
			curl_easy_setopt( handle, CURLOPT_WRITEDATA, file) ;
			printf( "Downloading JSON data from %s\n", JSON_DATA_URL );
			if( 0 == curl_easy_perform( handle ) )
			{
				printf( "Download successful\n" );
				curl_easy_cleanup( handle );
				downloadFlag = true;
			}
			fclose(file);
			if( downloadFlag == true )
			{
				parseJsonData( fileName );
			}
		}
    }
    else
    {
        printf( " Curl initialization failed\n" );
    }

}

void parseJsonData( char *fileName )
{
	struct json_object *recObject, *recArray;
	int len, i;
	recObject = json_object_from_file( fileName );
	recArray = json_object_get( recObject );
	len = json_object_array_length( recArray );
	DRIVERS *driverList = malloc( len * sizeof( DRIVERS ) );
	printf( "Drivers Length = %d\n", len );
	
	for( i = 0; i < len; i++ )
	{
		struct json_object *recArrayObj;
		recArrayObj = json_object_array_get_idx( recArray, i );
			
		driverList[i].id = json_object_get_int( json_object_object_get( recArrayObj, "id" ) );
		driverList[i].firstName = json_object_get_string( json_object_object_get( recArrayObj, "first_name" ) );
		driverList[i].lastName = json_object_get_string( json_object_object_get( recArrayObj, "last_name" ) );
		driverList[i].key = json_object_get_int( json_object_object_get( recArrayObj, "key" ) );
	}
	createAndDisplayData( len, driverList );
}

void createAndDisplayData( int len, DRIVERS *driverList )
{
	sqlite3 *db;
	char *error = NULL;
	int res, i;
	char *createTable = NULL;
	char *insertRec = NULL;
	char *displayContent = NULL;
	
	//Open database file. If not present, create one
	res = sqlite3_open( "records.db", &db );
	if( res )
	{
		printf( "Can't open database: %s\n", sqlite3_errmsg( db ) );
	}
   else
	{
		//Create table Drivers
		createTable = TABLE_CREATE;
		res = sqlite3_exec( db, createTable, callback, 0, &error );
		if( res != SQLITE_OK )
		{
			printf( "SQL Error: %s\n", error );
			sqlite3_free( error );
		}
		else
		{			
			//Insert records into table
			for( i = 0; i <len; i++ )
			{
				asprintf( &insertRec, "INSERT INTO %s ( id, first_name, last_name, key ) VALUES( %d, '%s', '%s', %d )",
																	DRIVER_TABLE_NAME, driverList[i].id,
																	driverList[i].firstName, driverList[i].lastName, driverList[i].key );
				res = sqlite3_exec( db, insertRec, callback, 0, &error );
				free( insertRec );
				if( res != SQLITE_OK )
				{
					printf( "SQL Error: %s\n", error );
					sqlite3_free( error );
					break;
				}
			}
			if( res == SQLITE_OK )
			{				
				//Display Content of table
				asprintf( &displayContent, "SELECT last_name, first_name, key from %s", DRIVER_TABLE_NAME );
				res = sqlite3_exec( db, displayContent, callback, 0, &error );
				if( res != SQLITE_OK )
				{
					printf( "SQL Error: %s\n", error );
					sqlite3_free( error );
				}
				else
				{
					printf("Table created successfully\n");
				}
			}
		}
	}
	sqlite3_close(db);
}

int main()
{
    downloadData( JSON_DATA_URL, "data.json" ) ;
    return 0;
}