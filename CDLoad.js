const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const odbc = require('odbc'); // For ODBC connection

// Path to the service account key file
const SERVICE_ACCOUNT_FILE = path.join(__dirname, 'service.json');
const SPREADSHEET_ID = '1biZdSLxU1Bk3ITbbGQ_GzPxuZu5CvnXxqKoELLpedf4'; // Replace with your Google Sheets ID

// ODBC Connection parameters with username and password
const ODBC_CONNECTION_STRING = 'DSN=Paragon;UID=lmerrill;PWD=lmerrill';


// SQL Query
const SQL_QUERY = `
  SELECT 
    b.pluplu as SKU, 
    b.pludesc, 
    a.mixmloy as Insider, 
    a.mixid, 
    a.MIXDESC,
    substring(a.MIXBCYMD, 5, 2) concat '/' concat substring(a.MIXBCYMD, 7, 2) concat '/' concat substring(a.MIXBCYMD, 1, 4) as begin, 
    substring(a.MIXECYMD, 5, 2) concat '/' concat substring(a.MIXECYMD, 7, 2) concat '/' concat substring(a.MIXECYMD, 1, 4) as end, 
    b.plurtl, 
    a.mixcmin as MinimumPurchase, 
    a.mixcpct2 as PctValue, 
    a.MIXCMAX as MaxAmount
  FROM ghmix2 a, ghplu b
  WHERE 
    right(cast((mixid+10000) as char(5)),4) = B.pluucls 
    AND a.mixecymd >= Cast(VarChar_Format(curdate(), 'YYYYMMDD') as dec(8, 0)) 
    AND a.mixtype = 'C'
    AND a.mixid <> 5096
  ORDER BY b.pluplu
`;

// Authorize and create the sheets client
const authorize = async () => {
  const credentials = JSON.parse(fs.readFileSync(SERVICE_ACCOUNT_FILE));
  const auth = new google.auth.JWT(
    credentials.client_email,
    null,
    credentials.private_key,
    ['https://www.googleapis.com/auth/spreadsheets']
  );
  await auth.authorize();
  return auth;
};

// Fetch data from the DB400 database
const fetchDatabaseData = async () => {
  try {
    const connection = await odbc.connect(ODBC_CONNECTION_STRING);
    const result = await connection.query(SQL_QUERY);
    await connection.close();

    // Get headers from the SQL result
    const headers = Object.keys(result[0]); // Use the keys of the first row as headers
    const rows = [headers, ...result.map(row => Object.values(row))]; // Add headers as the first row
    //console.log('Fetched data from database with headers:', rows); // Uncomment this to see data being loaded
    return rows;
  } catch (error) {
    console.error('Error fetching data from database:', error);
    throw error;
  }
};


// Clear the sheet
const clearSheet = async (auth) => {
  const sheets = google.sheets({ version: 'v4', auth });
  const range = 'Sheet1'; // Adjust the sheet name if necessary
  try {
    const response = await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: range,
    });
    console.log('Sheet cleared successfully:', response.data);
  } catch (error) {
    console.error('Error clearing the sheet:', error);
  }
};

// Insert data into Google Sheets
const insertIntoGoogleSheet = async (auth, rows) => {
  const sheets = google.sheets({ version: 'v4', auth });
  const range = 'Sheet1!A1'; // Adjust the range as needed

  const request = {
    spreadsheetId: SPREADSHEET_ID,
    range: range,
    valueInputOption: 'RAW',
    resource: {
      values: rows,
    },
  };

  try {
    const response = await sheets.spreadsheets.values.append(request);
    console.log('Data appended to the sheet:', response.data);
  } catch (err) {
    if (err.response) {
      console.error('Error appending data to the sheet:', err.response.data.error);
    } else {
      console.error('Error appending data to the sheet:', err);
    }
  }
};
// get Sheet id
const getSheetIdByName = async (auth, sheetName) => {
    const sheets = google.sheets({ version: 'v4', auth });
    try {
      const response = await sheets.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID
      });
      const sheet = response.data.sheets.find(s => s.properties.title === sheetName);
      return sheet ? sheet.properties.sheetId : null;
    } catch (error) {
      console.error('Error retrieving sheet ID:', error);
      return null;
    }
  };


// Main function
const main = async () => {
  try {
    const auth = await authorize();
    await clearSheet(auth); // Clear the sheet before inserting data
    const rows = await fetchDatabaseData();
    await insertIntoGoogleSheet(auth, rows);
  } catch (error) {
    console.error('Error in the main function:', error);
  }
};
 
  main().catch(console.error);

