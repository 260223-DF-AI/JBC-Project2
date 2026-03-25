/* 

This file is responsible for interacting with the Python FastAPI server 4
& displaying the retrieved data

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
        const response = await fetch(`${API_BASE_URL}/convert/?data_folder=data/`, {
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
        // Simulating a successful API call with placeholder data
        setTimeout(() => {
            const mockBigQueryData = [
                { id: 201, threat_actor: "Ransomware Syndicate", target_system: "File Storage", reputational_risk: "Critical", mitigation_status: "Unmitigated" },
                { id: 202, threat_actor: "Hacktivist Group", target_system: "Main Application", reputational_risk: "Medium", mitigation_status: "In Progress" }
            ];
            renderTable(mockBigQueryData);
            setStatus('Query success. Data loaded.', 'text-green-600');
        }, 500); 

    } catch (error) {
        console.error('Query Error:', error);
        setStatus('Failed to fetch query.', 'text-red-600');
    }
}

// Function to dynamically build the table
function renderTable(dataArray) {
    const tableHead = document.getElementById('table-head');
    const tableBody = document.getElementById('table-body');
    
    tableHead.innerHTML = '';
    tableBody.innerHTML = '';

    if (!dataArray || dataArray.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="100%" class="px-6 py-8 text-center text-gray-500">No data found</td></tr>';
        return;
    }

    // Create Headers
    const headers = Object.keys(dataArray[0]);
    let headerRow = '<tr>';
    headers.forEach(header => {
        const formattedHeader = header.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        headerRow += `<th scope="col" class="px-6 py-3 font-semibold text-gray-900">${formattedHeader}</th>`;
    });
    headerRow += '</tr>';
    tableHead.innerHTML = headerRow;

    // Create Rows
    dataArray.forEach(rowItem => {
        let row = '<tr class="hover:bg-gray-50 transition-colors">';
        headers.forEach(header => {
            let cellData = rowItem[header];
            
            // Tailwind formatting logic for specific text
            if (cellData === 'Critical') {
                cellData = `<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">${cellData}</span>`;
            } else if (cellData === 'High') {
                cellData = `<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-800">${cellData}</span>`;
            } else if (cellData === 'Medium') {
                cellData = `<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">${cellData}</span>`;
            } else if (cellData === 'Low') {
                cellData = `<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">${cellData}</span>`;
            }
            
            row += `<td class="px-6 py-4 text-gray-600 border-t border-gray-100">${cellData}</td>`;
        });
        row += '</tr>';
        tableBody.innerHTML += row;
    });
}