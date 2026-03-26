import {convertData} from './http.js';

const header_caption = document.getElementById('header_caption')
header_caption.innerText += " " + new Date().toLocaleDateString();

// Generic query handler factory
function makeQueryHandler(endpoint) {
    return async function() {
        const statusEl = document.getElementById('status-message');
        const API_BASE_URL = 'http://localhost:8000';
        
        function setStatus(message, colorClass) {
            if (!statusEl) return;
            statusEl.textContent = message;
            statusEl.classList.remove('text-blue-600', 'text-green-600', 'text-red-600');
            if (colorClass) statusEl.classList.add(colorClass);
        }
        
        setStatus('Sending request...', 'text-blue-600');
        try {
            const params = window.requestParams || {};
            
            // Build query string from parameters
            const queryParams = new URLSearchParams();
            Object.entries(params).forEach(([key, value]) => {
                if (value) queryParams.append(key, value);
            });
            
            const queryString = queryParams.toString();
            const url = `${API_BASE_URL}${endpoint}${queryString ? '?' + queryString : ''}`;
            
            const response = await fetch(url, {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
                redirect: 'follow'
            });

            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

            let result;
            try {
                result = await response.json();
            } catch (jsonErr) {
                console.warn('Failed to parse JSON from endpoint', jsonErr);
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
    };
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

    const headers = Object.keys(dataArray[0]);
    let headerRow = '<tr>';
    headers.forEach(header => {
        const formattedHeader = header.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        headerRow += `<th scope="col" class="px-6 py-3 font-semibold text-gray-900">${formattedHeader}</th>`;
    });
    headerRow += '</tr>';
    tableHead.innerHTML = headerRow;

    dataArray.forEach((rowItem, i) => {
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

// Define available HTTP requests with their parameters
const requests = [
    {
        id: 'convert',
        label: 'POST /convert',
        method: 'POST',
        handler: convertData,
        parameters: [
            { name: 'data_folder', label: 'Data Folder', placeholder: 'e.g., data/', required: false }
        ]
    },
    {
        id: 'active',
        label: 'GET /query/active - Most Active Customers',
        method: 'GET',
        handler: makeQueryHandler('/query/active'),
        parameters: [
            { name: 'limit', label: 'Limit', placeholder: 'e.g., 4', required: false },
            { name: 'order_by', label: 'Order By', placeholder: 'ASC or DESC', required: false }
        ]
    },
    {
        id: 'discounts',
        label: 'GET /query/discounts - Discount Analysis',
        method: 'GET',
        handler: makeQueryHandler('/query/discounts'),
        parameters: [
            { name: 'limit', label: 'Limit', placeholder: 'e.g., 4', required: false },
            { name: 'order_by', label: 'Order By', placeholder: 'ASC or DESC', required: false }
        ]
    },
    {
        id: 'max_revenue_days',
        label: 'GET /query/max_revenue_days - Max Revenue Days',
        method: 'GET',
        handler: makeQueryHandler('/query/max_revenue_days'),
        parameters: [
            { name: 'limit', label: 'Limit', placeholder: 'e.g., 4', required: false },
            { name: 'order_by', label: 'Order By', placeholder: 'ASC or DESC', required: false }
        ]
    },
    {
        id: 'top_products',
        label: 'GET /query/top_products - Top Products',
        method: 'GET',
        handler: makeQueryHandler('/query/top_products'),
        parameters: [
            { name: 'rank', label: 'Rank', placeholder: 'e.g., 3', required: false },
            { name: 'order_by', label: 'Order By', placeholder: 'ASC or DESC', required: false }
        ]
    },
    {
        id: 'worst_stores',
        label: 'GET /query/worst_stores - Worst Stores',
        method: 'GET',
        handler: makeQueryHandler('/query/worst_stores'),
        parameters: [
            { name: 'limit', label: 'Limit', placeholder: 'e.g., 4', required: false },
            { name: 'order_by', label: 'Order By', placeholder: 'ASC or DESC', required: false }
        ]
    }
];

const dropdown = document.getElementById('requestDropdown');
const parametersContainer = document.getElementById('parametersContainer');
const executeButton = document.getElementById('executeButton');

// Populate dropdown with request options
requests.forEach(request => {
    const option = document.createElement('option');
    option.value = request.id;
    option.textContent = request.label;
    dropdown.appendChild(option);
});

// Handle dropdown change
dropdown.addEventListener('change', (e) => {
    parametersContainer.innerHTML = '';
    executeButton.disabled = true;
    
    if (!e.target.value) return;
    
    const selectedRequest = requests.find(r => r.id === e.target.value);
    
    // Create textboxes for parameters
    selectedRequest.parameters.forEach(param => {
        const paramDiv = document.createElement('div');
        paramDiv.className = 'flex items-center gap-2';
        
        const label = document.createElement('label');
        label.textContent = param.label + ':';
        label.className = 'font-medium text-gray-700 w-32';
        
        const input = document.createElement('input');
        input.type = 'text';
        input.name = param.name;
        input.placeholder = param.placeholder;
        input.className = 'flex-1 px-3 py-2 border border-gray-300 rounded-lg text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500';
        input.required = param.required;
        
        paramDiv.appendChild(label);
        paramDiv.appendChild(input);
        parametersContainer.appendChild(paramDiv);
    });
    
    executeButton.disabled = false;
});

// Handle execute button
executeButton.addEventListener('click', async () => {
    const selectedId = dropdown.value;
    if (!selectedId) return;
    
    const selectedRequest = requests.find(r => r.id === selectedId);
    
    // Collect parameter values
    const inputs = parametersContainer.querySelectorAll('input');
    const params = {};
    inputs.forEach(input => {
        params[input.name] = input.value;
    });
    
    // Store params globally for the handler to access
    window.requestParams = params;
    
    // Call the appropriate handler
    selectedRequest.handler();
});

