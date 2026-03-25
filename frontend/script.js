import {convertData, queryData} from './http.js';


const header_caption = document.getElementById('header_caption')
header_caption.innerText += " " + new Date().toLocaleDateString();

const convertButton = document.getElementById('convertButton')
convertButton.onclick = convertData

const getQueryButton = document.getElementById('getQueryButton')
getQueryButton.onclick = queryData
