import ballerina/http;
import ballerina/log;
import ballerina/io;
import ballerina/sql;
import ballerina/mysql;


//jdbc:Client DBMysql = new({
//        url: "jdbc:mysql://localhost:3306/testdb_ballerina",
//        username: "root",
 //       password: "massimo",
//       poolOptions: { maximumPoolSize: 5 },
//        dbOptions: { useSSL: false,  serverTimezone: "UTC" }
//    });
    // Create a client endpoint for MySQL database. Change the DB details before running the sample.
mysql:Client DBMysql = new({
        host: "localhost",
        port: 3306,
        name: "testdb_ballerina",
        username: "root",
        password: "massimo"
    });
    mysql:Client DBMysql = new({
            host: "localhost",
            port: 3306,
            name: "testdb",
            username: "test",
            password: "test",
            dbOptions: { useSSL: false }
        });
  http:Client clientEndpoint = new("https://api.openweathermap.org");


public function main(string... args) {

    //http:Request req = new;
    // Send a GET request to the specified endpoint.
    var response = clientEndpoint->get("/data/2.5/weather?id=3166076&appid=4fd482904a9d92d2acb0e7d428e83ef6");
    log:printDebug("debug log");
    if (response is http:Response) {

        string contentType = response.getHeader("Content-Type");
        log:printInfo("Content-Type: " + contentType);

        int statusCode = response.statusCode;
        log:printInfo("Status code: " + statusCode);

        var msg = response.getJsonPayload();
        if (msg is json) {

            json jsonPayload = msg;
            io:println(jsonPayload);

            var cityid = <int>jsonPayload.id;
            var cityname = <string>jsonPayload.name;
            var temperature = <float>jsonPayload.main.temp;
            temperature = temperature - 273.15;
            var pressure = <int>jsonPayload.main.pressure;
            var humidity = <int>jsonPayload.main.humidity;
            var visibility = <int>jsonPayload.visibility;
            var windspeed_float = 0.0;
             if (jsonPayload.wind.speed is float) {
                windspeed_float = <float>jsonPayload.wind.speed;

                }
            else {
               windspeed_float = <float><int>jsonPayload.wind.speed;
                io:println("error wind.speed not present"); }
            var winddeg =0;
            if (jsonPayload.wind.deg is  int) {
                winddeg = <int>jsonPayload.wind.deg;
            }  else { io:println("error wind.deg not present"); }
            //var clouds = <int>jsonPayload.clouds.all;
            var weather = <string>jsonPayload.weather[0].main;


            // Inserts data to the table using the update action.
            sql:Parameter p1 = { sqlType: sql:TYPE_INTEGER, value: cityid };
            sql:Parameter p2 = { sqlType: sql:TYPE_VARCHAR, value: cityname };
            sql:Parameter p3 = { sqlType: sql:TYPE_DECIMAL, value: temperature };
            sql:Parameter p4 = { sqlType: sql:TYPE_SMALLINT, value: pressure };
            sql:Parameter p5 = { sqlType: sql:TYPE_SMALLINT, value: humidity };
            sql:Parameter p6 = { sqlType: sql:TYPE_SMALLINT, value: visibility };
            sql:Parameter p7 = { sqlType: sql:TYPE_DECIMAL, value: windspeed_float };
            sql:Parameter p8 = { sqlType: sql:TYPE_SMALLINT, value: winddeg };
            //sql:Parameter p9 = { sqlType: sql:TYPE_SMALLINT, value: clouds };
            sql:Parameter p10 = { sqlType: sql:TYPE_VARCHAR, value: weather };
            sql:Parameter p11 = { sqlType: sql:TYPE_BLOB, value: <any>response.getBinaryPayload() };




            io:println("\nThe update operation - Inserting data to a table");
            var ret = DBMysql->update("INSERT INTO openweathermap(cityid, cityname, temp, pressure, humidity, visibility, windspeed, winddeg , weather, json_)
                                     values (?,?,?,?,?,?,?,?,?,?)", p1,p2,p3,p4,p5,p6,p7,p8,p10,p11);
            handleUpdate(ret, "Insert to openweathermap table ");

        } else {
            log:printError(<string>msg.detail().message, err = msg);
        }
    } else {
        log:printError(<string>response.detail().message, err = response);
    }

}

	function handleUpdate(sql:UpdateResult|error returned, string message) {
        if (returned is sql:UpdateResult) {
            io:println(message + " status: " + returned.updatedRowCount);
        } else {
            io:println(message + " failed: " + <string>returned.detail().message);
        }
    }

