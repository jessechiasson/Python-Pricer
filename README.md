# Python Pricer

  This is a Python script that fetches stock price data from (in this case) twelvedata.com and parses it into a MySQL database in the desired format. You can use any data provider and any other database if you should prefer one to MySQL. 

## Notes

> This script uses and cycles through multiple free API keys (stored in a seperate file for safety) instead of paying for a subscription key. This is totally optional it was just included in case its necessary. 

> In some cases the database tables need to be created before hand. This would be a simple implementation inside the script using the PyMySQL library directly. 

> Any additional calculations and data manipulation can be added in once the data is returned from the API and ready to parse. 
