const express = require("express");
const router = express.Router();
const AdmZip = require("adm-zip");
const request = require("request");
const parse = require("csv-parse");
const ipaddr = require("ipaddr.js");
const mysql = require("mysql2/promise");
const db = mysql.createPool({
  host: "localhost",
  user: "root",
  password: "",
  database: "website",
  connectionLimit: 30// SSD NVME, what a breeze!
});
let geoLocations = {};

function downloadGeoIpZip() {
  return new Promise((resolve, reject) =>
    request(
      "https://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip",
      {
        encoding: null
      },
      (error, response, body) => {
        if (error || response.statusCode !== 200) reject("Couldn't download zip file");
        resolve(body);
      }
    )
  );
}

function fillCountryArr(record) {
  const country = geoLocations[record[1] || record[2]];
  if (!country) return;
  const ipCidr = ipaddr.parseCIDR(record[0]);
  const byteArr = ipCidr[0].toByteArray();
  const startAddress = byteArr.reduce((output, byte) => output + byte.toString(16).padStart(2, "0"), "");
  let endAddress = byteArr;
  const octetToPreserve = Math.ceil(ipCidr[1] / 8);
  let i = endAddress.length;
  while (--i >= octetToPreserve) endAddress[i] = 255;
  let bitsToFill = ipCidr[1] % 8;
  if (bitsToFill) {
    bitsToFill = 8 - bitsToFill;
    endAddress[i] |= Math.pow(2, bitsToFill) - 1;
  }
  endAddress = endAddress.reduce((output, byte) => output + byte.toString(16).padStart(2, "0"), "");
  // TODO: Batching inserts ie 50 entries per query can reduce load
  // NOTE: Can throw errors if server is weak. However 2 cores, 4GB RAM and SSD is fine for it.
  db.query(
    `INSERT INTO ipToCountry(startAddress,endAddress,country) VALUES (X'${startAddress}',X'${endAddress}','${country}')`
  );
}

function parseCSV(csv, recordCallback) {
  return new Promise(resolve =>
    parse(csv, {
      trim: true,
      from_line: 2
    })
      .on("readable", function() {
        let record;
        while ((record = this.read())) recordCallback(record);
      })
      .on("end", resolve)
  );
}

router.get("/down", async (req, res, next) => {
  try {
    await db.query(
      `CREATE TABLE IF NOT EXISTS ipToCountry (
        startAddress VARBINARY(16) NOT NULL,
        endAddress VARBINARY(16) NOT NULL,
        country VARCHAR(2) NOT NULL,
        PRIMARY KEY (startAddress, endAddress)
      )`
    );
    await db.query("TRUNCATE ipToCountry");
    // right now it's not a cron but we can make it so, to update the table per month
    res.send("Cron In Process");

    const admZip = new AdmZip(await downloadGeoIpZip());
    const entries = admZip.getEntries().map(entry => entry.entryName);

    const englishFilename = entries.find(entry => entry.endsWith("GeoLite2-Country-Locations-en.csv"));
    if (!englishFilename) throw "Couldn't find english csv";

    const ipv4Filename = entries.find(entry => entry.endsWith("GeoLite2-Country-Blocks-IPv4.csv"));
    if (!ipv4Filename) throw "Couldn't find IPv4 csv";

    const ipv6Filename = entries.find(entry => entry.endsWith("GeoLite2-Country-Blocks-IPv6.csv"));
    if (!ipv6Filename) throw "Couldn't find IPv6 csv";

    const readAsync = fileName => new Promise(resolve => admZip.readAsTextAsync(fileName, resolve));

    await parseCSV(await readAsync(englishFilename), record => (geoLocations[record[0]] = record[4] || record[2]));

    await parseCSV(await readAsync(ipv4Filename), fillCountryArr);

    await parseCSV(await readAsync(ipv6Filename), fillCountryArr);
  } catch (err) {
    next(err);
  }
});

router.get("/check", async (req, res, next) => {
  try {
    console.time("query");
    const [rows] = await db.query(
      `SELECT country FROM ipToCountry WHERE INET6_ATON(?) BETWEEN startAddress AND endAddress ORDER BY startAddress DESC LIMIT 1`,
      '79.183.87.219'
    );
    console.timeEnd("query");
    res.send(rows);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
