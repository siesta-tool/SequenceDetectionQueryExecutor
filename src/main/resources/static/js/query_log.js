let chartInstance = null;

function updateEventInfo(){
    const select = document.getElementById("eventList");
    const selectedEvent = select.value;

    const eventNameEl = document.getElementById("event-name");
    const eventCountEl = document.getElementById("event-occurrences");
    const infoCardEl = document.getElementById("event-info-card");
    const graphSelector = document.getElementById("graph-selector");
    const graphContainer = document.getElementById("graph-container");

    if (selectedEvent) {
        eventNameEl.textContent = selectedEvent;
        fetch(`/total_occurrences?logname=${window.tabOpen}&event_type=${selectedEvent}`)
            .then(res=>{
                return res.json()})
            .then(data=>{
                eventCountEl.textContent= data;
            })
            .catch(err => {
                console.error("Failed to load graph data", err);
                graphContainer.innerHTML = "<span style='color:red;'>Failed to load graph.</span>";
            })
        infoCardEl.style.display = "block";
        graphSelector.value="";
        graphContainer.style.display = "none";
    } else {
        infoCardEl.style.display = "none";
    }
}

function handleGraphSelection(){

    const selectorGraph = document.getElementById("graph-selector");
    const selectedGraph = selectorGraph.value;
    const graphContainer = document.getElementById("graph-container");
    const select = document.getElementById("eventList");
    const selectedEvent = select.value;


    if (!selectedGraph || !selectedEvent) {
        graphContainer.style.display = "none";
        return;
    } else {
        graphContainer.style.display = "flex";
    }


    // Build localStorage key
    const storageKey = getStorageKey(window.tabOpen, selectedEvent, selectedGraph);

    // Check if data is already cached
    const cachedData = localStorage.getItem(storageKey);
    if (cachedData) {
        const parsed = JSON.parse(cachedData);
        renderGraph(parsed, selectedGraph); // Your rendering function
        return;
    }

    // Otherwise fetch from backend
    selectorGraph.disabled = true;

    fetch(`/ui/event-graph-data?logname=${window.tabOpen}&graph=${selectedGraph}&event_type=${selectedEvent}`)
        .then(res => res.json())
        .then(data => {
            // Save to localStorage
            localStorage.setItem(storageKey, JSON.stringify(data));
            renderGraph(data, selectedGraph); // Render your graph
        })
        .catch(err => {
            console.error("Failed to load graph data", err);
            graphContainer.innerHTML = "<span style='color:red;'>Failed to load graph.</span>";
        })
        .finally(() => {
            selectorGraph.disabled = false;
        });

}

window.getStorageKey = function(logname, eventType, graphType) {
    return `${logname}::${eventType}::${graphType}`;
}

window.renderGraph=function (data, graphType) {
    const ctx = document.getElementById('event-graph-canvas').getContext('2d');

    const progressBar = document.getElementById("graph-progress-bar");
    const graphCanvas = document.getElementById('event-graph-canvas');
    graphCanvas.style.display='none';
    progressBar.style.display='block';


    // Clean up existing chart if any
    if (chartInstance) {
        chartInstance.destroy();
    }

    // Prepare labels and values
    const labels = Object.keys(data).sort((a, b) => Number(a) - Number(b));
    const values = labels.map(key => data[key]);

    chartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Instances per Trace',
                data: values,
                backgroundColor: 'rgba(63, 81, 181, 0.6)',
                borderColor: 'rgba(63, 81, 181, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                tooltip: {
                    enabled: true,
                    callbacks: {
                        label: function (ctx) {
                            return `Count: ${ctx.parsed.y}`;
                        }
                    }
                },
                zoom: {
                    pan: {
                        enabled: true,
                        mode: 'x'
                    },
                    zoom: {
                        wheel: {
                            enabled: true
                        },
                        pinch: {
                            enabled: true
                        },
                        mode: 'x'
                    }
                },
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Instances per Trace'
                    },
                    ticks: {
                        precision: 0
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Frequency'
                    }
                }
            }
        }
    });
    progressBar.style.display='none';
    graphCanvas.style.display='block';

}
