import ballerina/http;
import ballerina/log;
import ballerina/io;
import ballerinax/java.jdbc;

jdbc:Client DBMysql = new({
        url: "jdbc:mysql://localhost:3306/testdb_ballerina",
        username: "root",
       password: "massimo",
       poolOptions: { maximumPoolSize: 5 },
        dbOptions: { useSSL: false,  serverTimezone: "UTC" }
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
        log:printError(io:sprintf("Status code: " , statusCode));

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
            //var weather = <string>jsonPayload.weather.main;


            // Inserts data to the table using the update action.
            jdbc:Parameter p1 = { sqlType: jdbc:TYPE_INTEGER, value: cityid };
            jdbc:Parameter p2 = { sqlType: jdbc:TYPE_VARCHAR, value: cityname };
            jdbc:Parameter p3 = { sqlType: jdbc:TYPE_DECIMAL, value: temperature };
            jdbc:Parameter p4 = { sqlType: jdbc:TYPE_SMALLINT, value: pressure };
            jdbc:Parameter p5 = { sqlType: jdbc:TYPE_SMALLINT, value: humidity };
            jdbc:Parameter p6 = { sqlType: jdbc:TYPE_SMALLINT, value: visibility };
            jdbc:Parameter p7 = { sqlType: jdbc:TYPE_DECIMAL, value: windspeed_float };
            jdbc:Parameter p8 = { sqlType: jdbc:TYPE_SMALLINT, value: winddeg };
            //sql:Parameter p9 = { sqlType: sql:TYPE_SMALLINT, value: clouds };
            //jdbc:Parameter p10 = { sqlType: jdbc:TYPE_VARCHAR, value: weather };
            jdbc:Parameter p11 = { sqlType: jdbc:TYPE_BLOB, value: <anydata>response.getBinaryPayload() };




            io:println("\nThe update operation - Inserting data to a table");
            var ret = DBMysql->update("INSERT INTO openweathermap(cityid, cityname, temp, pressure, humidity, visibility, windspeed, winddeg , json_)
                                     values (?,?,?,?,?,?,?,?,?)", p1,p2,p3,p4,p5,p6,p7,p8,p11);
            handleUpdate(ret, "Insert to openweathermap table ");

        } else {
            log:printError(io:sprintf("Invalid payload received:" , msg.reason()));

        }
    } else {
         log:printError(io:sprintf("Error when calling the backend: ", response.reason()));
    }

}

	function handleUpdate(jdbc:UpdateResult|jdbc:Error returned, string message) {
    if (returned is jdbc:UpdateResult) {
        io:println(message, " status: ", returned.updatedRowCount);
    } else {
        error err = returned;
        io:println(message, " failed: ", <string> err.detail()["message"]);
    }
    }

