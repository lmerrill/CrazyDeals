const mysql = require('mysql2/promise');
const { google } = require('googleapis');
const fs = require('fs');

// Database connection details
const dbConfig = {
    host: 'proddatabase.c5yccmcgekf9.us-east-1.rds.amazonaws.com',
    user: 'ecomm', // Replace with your database username
    password: 'tmowrule2K!', // Replace with your database password
    database: 'oms'
};

// Google Sheets details
const SPREADSHEET_ID = '11CdGjW0mKkeELtRzstCljGP5ZjVVPC1j8ScXGxF0VD8';
const SHEET_NAME = 'tblProductEvents'; // Sheet name to use
const SERVICE_ACCOUNT_FILE = './service.json'; // Path to your service account JSON file

(async () => {
    try {
        // Step 1: Connect to the database and execute the query
        const connection = await mysql.createConnection(dbConfig);
        console.log('Connected to the RDS database.');

        const query = `
            SELECT cast(sku as int) as sku, eventId, eventTypeId, eventPrice, eventStartDate, eventEndDate, 
                   modifyDate, createDate, active
            FROM web.tblProductEvents
            WHERE active = 1
            order by cast(sku as int),
                     eventId;
        `;
        const [rows] = await connection.execute(query);
        console.log(`Number of rows selected: ${rows.length}`);

        // Step 2: Authenticate with Google Sheets API
        const auth = new google.auth.GoogleAuth({
            keyFile: SERVICE_ACCOUNT_FILE,
            scopes: ['https://www.googleapis.com/auth/spreadsheets']
        });
        const sheets = google.sheets({ version: 'v4', auth });

        // Step 3: Clear existing data in the sheet
        await sheets.spreadsheets.values.clear({
            spreadsheetId: SPREADSHEET_ID,
            range: `${SHEET_NAME}`
        });
        console.log(`Cleared existing data in the sheet "${SHEET_NAME}".`);

        // Step 4: Format the data for Google Sheets
        const values = rows.map(row => Object.values(row)); // Convert objects to arrays
        const headerRow = Object.keys(rows[0]); // Extract header from the first row
        values.unshift(headerRow); // Add header row to the top

        // Step 5: Update the Google Sheet with new data
        await sheets.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `${SHEET_NAME}!A1`, // Starting cell
            valueInputOption: 'RAW',
            requestBody: {
                values: values
            }
        });
        console.log(`Data successfully inserted into the Google Sheet "${SHEET_NAME}".`);

        // Step 6: Display the number of rows inserted
        const now = new Date();
        const timestamp = now.toLocaleString(); // Format: MM/DD/YYYY, HH:MM:SS
        const rowsInserted = values.length - 1; // Exclude the header row
        console.log(`${timestamp} - The number of rows inserted: ${rowsInserted}`);

        // Close the database connection
        await connection.end();
        console.log('Connection closed.');
    } catch (error) {
        console.error('Error:', error.message);
    }
})();
