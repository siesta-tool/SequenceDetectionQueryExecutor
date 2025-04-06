function updateEventInfo() {
    const select = document.getElementById("eventList");
    const selectedEvent = select.value;

    const eventNameEl = document.getElementById("event-name");
    const eventCountEl = document.getElementById("event-occurrences");
    const infoCardEl = document.getElementById("event-info-card");

    if (selectedEvent) {
        eventNameEl.textContent = selectedEvent;
        eventCountEl.textContent = eventStats[selectedEvent] !== undefined ? eventStats[selectedEvent] : "—";
        infoCardEl.style.display = "block";
    } else {
        infoCardEl.style.display = "none";
    }
}