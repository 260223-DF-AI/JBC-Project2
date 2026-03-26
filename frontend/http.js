/* 

This file is responsible for interacting with the 
FastAPI server & displaying the retrieved data

*/

const API_BASE_URL = 'http://localhost:8000';
const statusEl = document.getElementById('status-message');

function setStatus(message, colorClass) {
    if (!statusEl) return;
    statusEl.textContent = message;
    statusEl.classList.remove('text-blue-600', 'text-green-600', 'text-red-600');
    if (colorClass) statusEl.classList.add(colorClass);
}

// POST - /convert
export async function convertData() {
    setStatus('Sending /convert request...', 'text-blue-600');
    try {
        const params = window.requestParams || { data_folder: 'data/' };
        const dataFolder = params.data_folder || 'data/';
        
        const response = await fetch(`${API_BASE_URL}/convert/?data_folder=${encodeURIComponent(dataFolder)}`, {
            method: 'POST',
            headers: { 'Accept': 'application/json' },
            redirect: 'follow'
        });
        
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        let result = null;
        try {
            result = await response.json();
        } catch (jsonErr) {
            console.warn('No JSON payload from convert endpoint', jsonErr);
        }

        const payload = result ? JSON.stringify(result) : 'No response body';
        setStatus(`Convert success: ${payload}`, 'text-green-600');
    } catch (error) {
        console.error('Convert Error:', error);
        setStatus('Failed to reach API. Ensure FastAPI is running on localhost:8000.', 'text-red-600');
    }
}

// GET - /query
export async function queryData() {
    setStatus('Sending /query request...', 'text-blue-600');
    try {
        const params = window.requestParams || { limit: 4 };
        const limit = params.limit || 4;
        
        const response = await fetch(`${API_BASE_URL}/query/active?limit=${encodeURIComponent(limit)}`, {
            method: 'GET',
            headers: { 'Accept': 'application/json' },
            redirect: 'follow'
        });

        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

        let result;
        try {
            result = await response.json();
        } catch (jsonErr) {
            console.warn('Failed to parse JSON from query endpoint', jsonErr);
            setStatus('Invalid response format from server.', 'text-red-600');
            return;
        }

        // Handle case where result is a stringified JSON
        if (typeof result === 'string') {
            try {
                result = JSON.parse(result);
            } catch (parseErr) {
                console.error('Failed to parse stringified JSON:', parseErr);
                setStatus('Invalid response format from server.', 'text-red-600');
                return;
            }
        }

        // Validate result is an array before rendering
        console.log('Query result type:', typeof result, 'is array:', Array.isArray(result));
        
        let dataToRender = result;
        if (!Array.isArray(result)) {
            // Check if result has a data property that contains the array
            if (result && typeof result === 'object' && Array.isArray(result.data)) {
                dataToRender = result.data;
            } else {
                console.error('Query returned non-array data:', result);
                setStatus('Unexpected response format.', 'text-red-600');
                return;
            }
        }
        
        renderTable(dataToRender);
        setStatus('Query successful!', 'text-green-600');

    } catch (error) {
        console.error('Query Error:', error);
        setStatus('Failed to fetch query.', 'text-red-600');
    }
}


function renderTable(dataArray) {
    const tableHead = document.getElementById('table-head');
    const tableBody = document.getElementById('table-body');
    
    tableHead.innerHTML = '';
    tableBody.innerHTML = '';

    if (!dataArray || dataArray.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="100%" class="px-6 py-8 text-center text-gray-500">No data found</td></tr>';
        return;
    }

    // create headers
    const headers = Object.keys(dataArray[0]);
    let headerRow = '<tr>';
    headers.forEach(header => {
        const formattedHeader = header.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        headerRow += `<th scope="col" class="px-6 py-3 font-semibold text-gray-900">${formattedHeader}</th>`;
    });
    headerRow += '</tr>';
    tableHead.innerHTML = headerRow;

    // create rows
    dataArray.forEach((rowItem, i) => {
        // every other row's background is different for readability
        const bgColor = i % 2 === 0 ? 'bg-white' : 'bg-gray-50';
        let row = `<tr class="${bgColor} hover:bg-blue-50 transition-colors">`;
        
        headers.forEach(header => {
            let cellData = rowItem[header];
            row += `<td class="px-6 py-4 text-gray-600 border-t border-gray-100">${cellData}</td>`;
        });
        row += '</tr>';
        tableBody.innerHTML += row;
    });
}