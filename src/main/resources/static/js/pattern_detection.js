const events = [];
const symbols = ['_', '*', '+', '||','!'];


function loadEventsForLog() {
    const selectedLog = document.getElementById('logSelector').value;
    if (!selectedLog) return;

    const input = document.getElementById('event-input');
    const eventList = document.getElementById('event-list');
    const symbolList = document.getElementById('symbol-list');
    const tagsContainer = document.getElementById('tags');

    fetch(`/eventTypes?logname=${encodeURIComponent(selectedLog)}`)
        .then(response => response.json())
        .then(data => {
            // Update global events array
            events.length = 0; // clear the existing list
            events.push(...data); // populate with new ones
            input.value = '';
            eventList.innerHTML = '';
            tagsContainer.innerHTML = '';
            symbolList.classList.add('hidden');
            document.getElementById("pattern-search").classList.remove('hidden')
        })
        .catch(err => {
            console.error('Error fetching events:', err);
        });
}