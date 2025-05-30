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

async function loadStatsFragment() {
    console.log("Loading stats fragment...");
    function buildStatsPayload() {
        const logName = document.getElementById('logSelector').value;
        const eventNames = getAllTagEvents();

        const events = eventNames.map(name => ({ name }));

        return {
            log_name: logName,
            pattern: { events }
        };
    }

    const payload = buildStatsPayload();

    const response = await fetch("/fragments/pattern-stats", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    });

    const html = await response.text();
    document.getElementById("pattern-stats-fragment").innerHTML = html;
}

let constraintCount = 1;

async function addConstraint() {
    const container = document.getElementById("constraints-container");
    const addButton = document.getElementById("add-constraint-button");

    const index = constraintCount++;

    const response = await fetch(`/fragments/constraint-card?index=${index}`);
    const html = await response.text();

    const temp = document.createElement("div");
    temp.innerHTML = html;
    const card = temp.firstElementChild;

    container.insertBefore(card, addButton);

    populateEventDropdowns(card); // your logic to fill Event A/B
}

function updateAllConstraintDropdowns() {
    const tagEvents = getAllTagEvents();
    const cards = document.querySelectorAll('.constraint-card');

    cards.forEach(card => {
        const selectA = card.querySelector('.event-a');
        const selectB = card.querySelector('.event-b');


        const selectedA = selectA.value;
        const selectedB = selectB.value;

        const eventsA = [];
        const eventsB = [];
        // Clear and repopulate Event A
        selectA.innerHTML = `<option disabled selected value="">Select event</option>`;
        tagEvents.forEach((name, index) => {
            const option = document.createElement('option');
            option.value = `${name} (${index + 1})`;
            option.textContent = `${name} (${index + 1})`;
            eventsA.push(option.textContent);
            selectA.appendChild(option);
        });

        // Clear and repopulate Event B
        selectB.innerHTML = `<option disabled selected value="">Select event</option>`;
        tagEvents.forEach((name, index) => {
            if(index>0) {
                const option = document.createElement('option');
                option.value = `${name} (${index + 1})`;
                option.textContent = `${name} (${index + 1})`;
                eventsB.push(option.textContent);
                selectB.appendChild(option);
            }
        });

        // Re-select previously selected value (if still valid)
        if (eventsA.includes(selectedA)) {
            selectA.value = selectedA;
        }

        if (eventsB.includes(selectedB)) {
            selectB.value = selectedB;
        }
    });
}

function populateEventDropdowns(card) {
    const tagEvents = getAllTagEvents();

    const eventASelect = card.querySelector(".event-a");
    const eventBSelect = card.querySelector(".event-b");

    eventASelect.innerHTML = `<option disabled selected value="">Select event</option>`;
    eventBSelect.innerHTML = `<option disabled selected value="">Select event</option>`;

    // Populate Event A and B
    tagEvents.forEach((name, index) => {
        const label = `${name} (${index + 1})`;

        const optionA = document.createElement("option");
        optionA.value = index;
        optionA.textContent = label;
        eventASelect.appendChild(optionA);

        const optionB = document.createElement("option");
        optionB.value = index;
        optionB.textContent = label;
        eventBSelect.appendChild(optionB);
    });
}

function removeConstraint(btn) {
    const card = btn.closest(".constraint-card");
    constraintCount--;
    card.remove();
}

function toggleSingle(btn) {
    const siblings = btn.parentElement.querySelectorAll(".toggle-btn");
    siblings.forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
}

function toggleConstraintType(btn) {
    toggleSingle(btn);
    const container = btn.closest(".constraint-card");
    const isTime = btn.textContent.includes("Time");

    const granularity = container.querySelector(".granularity-select");


    if (granularity) {
        if (isTime) {
            granularity.classList.remove("invisible-space");
        } else {
            granularity.classList.add("invisible-space");
        }
    }
}
