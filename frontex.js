var highland = require('highland')
var request = require('request')
var cheerio = require('cheerio')
var fs = require('fs')
var csvWriter = require('csv-write-stream')

var http = highland.wrapCallback(function (location, callback) {
    request(location, function (error, response) {
	var failure = error ? error : (response.statusCode >= 400) ? new Error(response.statusCode) : null
	callback(failure, response)
    })
})

var archive = 'http://frontex.europa.eu/operations/archive-of-operations/'

function pages(response) {
    var document = cheerio.load(response.body)
    var pages = document('.pagination li:nth-last-child(1) a').text()
    return Array.apply(null, {length: pages}).map(Number.call, Number).map(function (n) {
	return response.request.href + '?p=' + (n + 1)
    })
}

function operations(response) {
    var document = cheerio.load(response.body)
    return document('.operations-list a').get().map(function (operation) {
	return 'http://' + response.request.host + cheerio(operation).attr('href')
    })
}

function details(response) {
    var document = cheerio.load(response.body)
    return {
	location: response.request.href,
	name: document('dd:nth-of-type(2)').text(),
	year: document('dd:nth-of-type(3)').text(),
	hostCountry: document('dd:nth-of-type(4)').text(),
	participatingCountry: document('dd:nth-of-type(5)').text(),
	type: document('dd:nth-of-type(6)').text(),
	region: document('dd:nth-of-type(7)').text(),
	budget: document('dd:nth-of-type(8)').text(),
	realisation: document('dd:nth-of-type(9)').text()
    }
}

highland([archive])
    .flatMap(http)
    .flatMap(pages)
    .flatMap(http)
    .flatMap(operations)
    .flatMap(http)
    .map(details)
    .errors(function (e) { console.log('Error: ' + e.message) })
    .through(csvWriter())
    .pipe(fs.createWriteStream('frontex.csv'))
