import ballerina/http;
import ballerina/log;
import ballerina/io;
import ballerina/mysql;
import ballerina/sql;

endpoint http:Client clientEndpoint {
    url: "https://api.openweathermap.org"
};
endpoint mysql:Client testDB { host: "localhost", port: 3306, name: "testdb_ballerina", username: "root",  password: "massimo",  
	poolOptions: { maximumPoolSize: 5 },
    dbOptions: { useSSL: false },
	dbOptions: { serverTimezone: "UTC" }
};

function main(string... args) {

    http:Request req = new;
    // Send a GET request to the specified endpoint.
    var response = clientEndpoint->get("/data/2.5/weather?id=3166076&appid=4fd482904a9d92d2acb0e7d428e83ef6");

    match response {
        http:Response resp => {
            string contentType = resp.getHeader("Content-Type");
            log:printInfo("\nContent-Type: " + contentType);

            int statusCode = resp.statusCode;
            log:printInfo("Status code: " + statusCode);

            
            log:printInfo("GET request:");
            var msg = resp.getJsonPayload();
            match msg {
                json jsonPayload => {
					
					int cityidF ;
                    var cityid = <int>jsonPayload.id;
                    match cityid {
                        int value => cityidF=value;
                         error err => io:println("error: " + err.message);
                    }
					
					string citynameF ;
                    var cityname = <string>jsonPayload.name;
                    match cityname {
                        string value => citynameF =value;
                         error err => io:println("error: " + err.message);
                    }
				
                    float temperatureF ;
                    var temperature = <float>jsonPayload.main.temp;
                    match temperature {
                        float value => temperatureF=value;
                         error err => io:println("error: " + err.message);
                    }
                     temperatureF = temperatureF - 273.15;
                    var strVal = <string>temperatureF;
                    log:printInfo(jsonPayload.toString());
					
					int pressureF ;
                    var pressure = <int>jsonPayload.main.pressure;
                    match pressure {
                        int value => pressureF=value;
                         error err => io:println("error: " + err.message);
                    }
					
					int humidityF ;
                    var humidity = <int>jsonPayload.main.humidity;
                    match humidity {
                        int value => humidityF=value;
                         error err => io:println("error: " + err.message);
                    }
					
					int visibilityF ;
                    var visibility = <int>jsonPayload.visibility;
                    match visibility {
                        int value => visibilityF=value;
                         error err => io:println("error: " + err.message);
                    }
					
					float windspeedF ;
                    var windspeed = <float>jsonPayload.wind.speed;
                    match windspeed {
                        float value => windspeedF =value;
                         error err => io:println("error: " + err.message);
                    }
					
					int winddegF ;
                    var winddeg = <int>jsonPayload.wind.deg;
                    match winddeg {
                        int value => winddegF=value;
                         error err => io:println("error: " + err.message);
                    }
					
					int cloudsF ;
                    var clouds = <int>jsonPayload.clouds;
                    match clouds {
                        int value => cloudsF=value;
                         error err => io:println("error: " + err.message);
                    }
					
					string weatherF ;
                    var weather = <string>jsonPayload.weather[0].main;
                    match weather {
                        string value => weatherF =value;
                         error err => io:println("error: " + err.message);
                    }
					
					    // Inserts data to the table using the update action.
						sql:Parameter p1 = { sqlType: sql:TYPE_INTEGER, value: cityidF };
						sql:Parameter p2 = { sqlType: sql:TYPE_VARCHAR, value: citynameF };
						sql:Parameter p3 = { sqlType: sql:TYPE_DECIMAL, value: temperatureF };
						sql:Parameter p4 = { sqlType: sql:TYPE_SMALLINT, value: pressureF };
						sql:Parameter p5 = { sqlType: sql:TYPE_SMALLINT, value: humidityF };
						sql:Parameter p6 = { sqlType: sql:TYPE_SMALLINT, value: visibilityF };
						sql:Parameter p7 = { sqlType: sql:TYPE_DECIMAL, value: windspeedF  };
						sql:Parameter p8 = { sqlType: sql:TYPE_SMALLINT, value: winddegF };
						sql:Parameter p9 = { sqlType: sql:TYPE_SMALLINT, value: cloudsF };
						sql:Parameter p10 = { sqlType: sql:TYPE_VARCHAR, value: weatherF };
						sql:Parameter p11 = { sqlType: sql:TYPE_BLOB, value: jsonPayload.toString().toBlob("UTF-8") };
						
						io:println("\nThe update operation - Inserting data to a table");
						var ret = testDB->update("INSERT INTO openweathermap(cityid, cityname, temp, pressure, humidity, visibility, windspeed, winddeg, clouds , weather, json_)
											  values (?,?,?,?,?,?,?,?,?,?,?)", p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11);
						handleUpdate(ret, "Insert to openweathermap table with no parameters");
            
                }
                error err => {
                    log:printError(err.message, err = err);
                }
            }
        }
        error err => { log:printError(err.message, err = err); }
    }
}

	// Function to handle return of the update operation.
	function handleUpdate(int|error returned, string message) {
		match returned {
			int retInt => io:println(message + " status: " + retInt);
			error e => io:println(message + " failed: " + e.message);
		}
	}