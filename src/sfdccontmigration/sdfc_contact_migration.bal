import ballerina/io;
import ballerina/config;
import ballerinax/mysql;
import ballerina/'log;
import ballerina/sql;
import ballerinax/sfdc;

sfdc:SalesforceConfiguration sfConfig = {
    baseUrl: config:getAsString("SF_EP_URL"),
    clientConfig: {
        accessToken: config:getAsString("SF_ACCESS_TOKEN"),
        refreshConfig: {
            clientId: config:getAsString("SF_CLIENT_ID"),
            clientSecret: config:getAsString("SF_CLIENT_SECRET"),
            refreshToken: config:getAsString("SF_REFRESH_TOKEN"),
            refreshUrl: config:getAsString("SF_REFRESH_URL")
        }
    }
};

sfdc:ListenerConfiguration listenerConfig = {
    username: config:getAsString("SF_USERNAME"),
    password: config:getAsString("SF_PASSWORD")
};

sfdc:BaseClient baseClient = new(sfConfig);

listener sfdc:Listener sfdcEventListener = new (listenerConfig);
mysql:Client mysqlClient =  check new (user = config:getAsString("DB_USER"),
                                        password = config:getAsString("DB_PWD"));

public function main(){
    string queryStr = "SELECT Id FROM Contact";
    //create bulk job
    error|sfdc:BulkJob queryJob = baseClient->creatJob("query", "Contact", "JSON");
    if (queryJob is sfdc:BulkJob) {
        //create batch
        error|sfdc:BatchInfo batch = queryJob->addBatch(queryStr);
        if (batch is sfdc:BatchInfo) {
            io:println(batch);
            string batchId = batch.id;
            //get batch result
            var batchResult = queryJob->getBatchResult(batchId);
            if (batchResult is json) {
                json[]|error batchResultArr = <json[]>batchResult;
                if (batchResultArr is json[]) {
                    foreach var result in batchResultArr {
                        string contactId = result.Id.toString();
                        log:printInfo("Contact ID : " + contactId); 
                        migrateContact(contactId);
                    }
                } else {
                    log:printError(batchResultArr.toString());
                }
            } else if (batchResult is error) {
                log:printError(batchResult.message());
            } else {
                log:printError("Invalid Batch Result!");
            }
            
        } else {
            log:printError(batch.message());
        }
    }
    else{
        log:printError(queryJob.message());
    }
}

@sfdc:ServiceConfig {
    topic:config:getAsString("SF_CONTACT_TOPIC")
}

service sfdcContactListener on sfdcEventListener {
    resource function onEvent(json cont) {  
        //convert json string to json
        io:StringReader sr = new(cont.toJsonString());
        json|error contact = sr.readJson();
        if (contact is json) {
            log:printInfo(contact.toJsonString());
            //Get the contact id from the contact
            string contactId = contact.sobject.Id.toString();
            log:printInfo("Contact ID : " + contactId);
            migrateContact(contactId);
        }
    }
}

function migrateContact(string contactId) {
    json|sfdc:Error contactInfo = baseClient->getContactById(contactId);
    if (contactInfo is json) {
        // Log contact information. 
        log:printInfo(contactInfo);
        // Add the current contact to a DB. 
        addContactToDB(<@untainted>contactInfo);
    }
}

function addContactToDB(json contact) {
    string id = contact.Id.toString();
    string salutation = contact.Salutation.toString();
    string name = contact.Name.toString();
    string mobilePhone = contact.MobilePhone.toString();
    string email = contact.Email.toString();
    string phone = contact.Phone.toString();
    string fax = contact.Fax.toString();
    string accountId = contact.AccountId.toString();
    string title = contact.Title.toString();
    string department = contact.Department.toString();
    
    log:printInfo(id + ":" + accountId + ":" + name + ":" + title);
    // The SQL query to insert a contact record to the DB. 
    sql:ParameterizedQuery insertQuery =
            `INSERT INTO ESC_SFDC_TO_DB.Contact (Id, Salutation, Name, Mobile, Email, Phone, Fax, AccountId, Title, Department) 
            VALUES (${id}, ${salutation}, ${name}, ${mobilePhone}, ${email}, ${phone}, ${fax}, ${accountId}, ${title}, ${department})`;
    // Invoking the MySQL Client to execute the insert operation. 
    sql:ExecutionResult|sql:Error? result  = mysqlClient->execute(insertQuery);
    if result is sql:Error{
        //if the entry exists in db 
        if result is sql:DatabaseError{
            sql:DatabaseErrorDetail errorDetails = result.detail();
            if(1062==errorDetails.errorCode){
                // The SQL query to update a contact record to the DB. 
                sql:ParameterizedQuery updateQuery =
                `UPDATE ESC_SFDC_TO_DB.Contact SET Salutation=${salutation}, Name=${name}, Mobile=${mobilePhone}, Email=${email}, Phone=${phone}, Fax=${fax}, AccountId=${accountId}, Title=${title}, Department=${department} WHERE Id=${id}`;
                sql:ExecutionResult|sql:Error? updateResult  = mysqlClient->execute(updateQuery);
                if updateResult is sql:Error{
                    log:printError(updateResult.message());
                }
            }
            else{
                log:printError(result.message());
            }
        }
        else{
            log:printError(result.message());
        }
        
    }
    
}
