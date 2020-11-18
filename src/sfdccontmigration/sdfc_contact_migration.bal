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
            json|sfdc:Error contactInfo = baseClient->getContactById(contactId);
            if (contactInfo is json) {
                // Log contact information. 
                log:printInfo(contactInfo);
                // Add the current contact to a DB. 
                sql:Error? result  = addContactToDB(<@untainted>contactInfo);
                if (result is error) {
                    log:printError(result.message());
                }
            }

        }
    }
}


function addContactToDB(json contact) returns sql:Error? {
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
    // The SQL query to insert an contact record to the DB. 
    sql:ParameterizedQuery insertQuery =
            `INSERT INTO ESC_SFDC_TO_DB.Contact (Id, Salutation, Name, Mobile, Email, Phone, Fax, AccountId, Title, Department) 
            VALUES (${id}, ${salutation}, ${name}, ${mobilePhone}, ${email}, ${phone}, ${fax}, ${accountId}, ${title}, ${department})`;
    // Invoking the MySQL Client to execute the insert operation. 
    sql:ExecutionResult result  =  check mysqlClient->execute(insertQuery);
}
