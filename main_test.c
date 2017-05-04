#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <curl/curl.h>
#include <json/json.h>
#include <sqlite3.h>
#include <string.h>
#include "task_defs.h"

//Function Declarations
void parseJsonData( char *fileName );
void downloadData( const char *url, char *fileName );
bool createTable();
void insertRecords( int recId, const char *recFirstName, const char *recLastName, int recKey );
void displayContent();

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
    //curl_global_init( CURL_GLOBAL_ALL );
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
	int len, i, dbResult = 0;
	bool crRes = false;
	
	recObject = json_object_from_file( fileName );
	recArray = json_object_get( recObject );
	len = json_object_array_length( recArray );
	printf( "Drivers Length = %d\n", len );
	crRes = createTable();
	if( false == crRes )
	{
		printf( " Database creation failed" );
	}
	else
	{
		for( i = 0; i < len; i++ )
		{
			struct json_object *recArrayObj, *recArrObjFirstName, *recArrObjLastName, *recArrObjID, *recArrObjKey;
			int key, ID;
			const char *firstName = NULL;
			const char *lastName = NULL;
			
			recArrayObj = json_object_array_get_idx( recArray, i );
			
			recArrObjID = json_object_object_get( recArrayObj, "id" );
			recArrObjFirstName = json_object_object_get( recArrayObj, "first_name" );
			recArrObjLastName = json_object_object_get( recArrayObj, "last_name" );
			recArrObjKey = json_object_object_get( recArrayObj, "key" );
			
			ID = json_object_get_int( recArrObjID );
			firstName = json_object_get_string( recArrObjFirstName );
			lastName = json_object_get_string( recArrObjLastName );
			key = json_object_get_int( recArrObjKey );
			
			insertRecords( ID, firstName, lastName, key );
		}
		displayContent();
	}
}

bool createTable()
{
	sqlite3 *db;
	char *error = NULL;
	int res;
	char *createTable;
	bool dbRes = true;
	res = sqlite3_open( "records.db", &db );
	if( res )
	{
		printf( "Can't open database: %s\n", sqlite3_errmsg( db ) );
		dbRes = false;
	}
   else
	{
		printf( "Opened database successfully\n" );
		createTable = TABLE_CREATE;
		res = sqlite3_exec( db, createTable, callback, 0, &error );
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
   sqlite3_close(db);
   return dbRes;
}

void insertRecords( int recId, const char *recFirstName, const char *recLastName, int recKey )
{
	sqlite3 *db;
	sqlite3_stmt *statement;
	char *error = NULL;
	int res;
	char *insertRec = NULL;
	bool dbRes = true;
	res = sqlite3_open( "records.db", &db );
	
	if( res )
	{
		printf( "Can't open database: %s\n", sqlite3_errmsg( db ) );
		dbRes = false;
	}
   else
	{
		asprintf( &insertRec, "INSERT INTO %s ( id, first_name, last_name, key ) VALUES( %d, '%s', '%s', %d )", DRIVER_TABLE_NAME, recId,
												recFirstName, recLastName, recKey );
		res = sqlite3_exec( db, insertRec, callback, 0, &error );
		if( res != SQLITE_OK )
		{
			printf( "SQL Error: %s\n", error );
			sqlite3_free( error );
		}
	}
   sqlite3_close(db);
}

void displayContent()
{
	sqlite3 *db;
	sqlite3_stmt *statement;
	char *error = NULL;
	int res;
	char *displayContent;
	bool dbRes = true;
	res = sqlite3_open( "records.db", &db );
	if( res )
	{
		printf( "Can't open database: %s\n", sqlite3_errmsg( db ) );
		dbRes = false;
	}
   else
	{
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
   sqlite3_close(db);
}

int main()
{
    downloadData( JSON_DATA_URL, "data.json" ) ;
    return 0;
}