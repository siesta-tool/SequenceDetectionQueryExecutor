const events = [];
const symbols = ['_', '*', '+', '||','!'];


function loadEventsForLog() {
    const selectedLog = document.getElementById('logSelector').value;
    sessionStorage.setItem("siesta_selected_log", selectedLog);
    if (!selectedLog) return;

    const input = document.getElementById('event-input');
    const eventList = document.getElementById('event-list');
    const symbolList = document.getElementById('symbol-list');
    const tagsContainer = document.getElementById('tags');

    fetch(`/eventTypes?logname=${encodeURIComponent(selectedLog)}`)
        .then(response => response.json())
        .then(data => {
            events.length = 0;
            events.push(...data);
            input.value = '';
            eventList.innerHTML = '';
            tagsContainer.innerHTML = '';
            symbolList.classList.add('hidden');
            document.getElementById("pattern-search").classList.remove('hidden')
            document.getElementById("pattern-filters").classList.remove('hidden')
            document.getElementById("pattern-stats").classList.remove('hidden')
            document.getElementById("pattern-search-button").classList.remove('hidden')
            document.getElementById("pattern-results-expandable-card").classList.remove('hidden')
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
    const resultCard = document.getElementById('pattern-stats');
    if (resultCard) {
        resultCard.classList.remove('collapsed');
        resultCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
        resultCard.classList.add('flash-highlight');

        setTimeout(() => {
            resultCard.classList.remove('flash-highlight');
        }, 2000); // duration must match CSS animation
    }
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

function checkFilterTagVisibility() {
    const tagContainer = document.getElementById('filter-tag-container');
    const wrapper = document.getElementById('pattern-filters-tags');
    if (tagContainer.children.length === 0) {
        wrapper.classList.add('hidden');
    } else {
        wrapper.classList.remove('hidden');
    }
}

function createFilterTag(id, label, value, json, onDeleteCallback) {
    const tag = document.createElement('div');
    tag.className = 'tag';
    tag.dataset.id = id;
    tag.dataset.json = JSON.stringify(json);

    const text = document.createElement('span');
    text.textContent = `${label}${value}`;

    const closeBtn = document.createElement('span');
    closeBtn.textContent = '×';
    closeBtn.className = 'close-btn';
    closeBtn.addEventListener('click', () => {
        tag.remove();
        if (typeof onDeleteCallback === 'function') {
            onDeleteCallback();
        }
        checkFilterTagVisibility();
    });

    tag.appendChild(text);
    tag.appendChild(closeBtn);
    document.getElementById('filter-tag-container').appendChild(tag);

    checkFilterTagVisibility();
}

function removeFilterTag(id) {
    const tag = document.querySelector(`.tag[data-id="${id}"]`);

    if (tag) {
        tag.remove();
    }
}

//hooks for filters
function saveFilters(){
    console.log("Saving filters");
    let fromValue = document.getElementById('date-from').value;
    let unixTime = new Date(fromValue).getTime();
    removeFilterTag('date-from'); // prevent duplicates
    if (fromValue) {
        createFilterTag('date-from', 'From: ', fromValue, {from:unixTime},() => {
            document.getElementById('date-from').value = '';
            checkFilterTagVisibility();
        });
    }
    let toValue = document.getElementById('date-to').value;
    unixTime = new Date(toValue).getTime();
    removeFilterTag('date-to');
    if (toValue) {
        createFilterTag('date-to', 'To: ', toValue,{till:unixTime} ,() => {
            document.getElementById('date-to').value = '';
            checkFilterTagVisibility();
        });
    }
    removeFilterTag('return-all');
    if(document.getElementById('return-all').checked){
        createFilterTag('return-all', 'Return All', '',{returnAll:true} ,() => {
            document.getElementById('return-all').checked = false;
            checkFilterTagVisibility();
        });
    }


    let groupDefinition = document.getElementById('groups').value.replaceAll(' ','');
    if(groupDefinition){
        if(isValidGroupDefinition(groupDefinition)){
            document.getElementById('groups').classList.remove('invalid-input');
            removeFilterTag('groups');
            createFilterTag(
                'groups',
                'Groups: ',
                groupDefinition,
                {"groups-config":groupDefinition},
                () => {
                    document.getElementById('groups').value = '';
                    checkFilterTagVisibility();
                }
            )
        }else{
            document.getElementById('groups').classList.add('invalid-input');
        }
    }


    const kValue = document.getElementById('k').value;
    const uncertaintyValue = document.getElementById('uncertainty').value;
    const kInput = document.getElementById('k');
    const uncertaintyInput = document.getElementById('uncertainty');

    const granularityUncertainty = document.getElementById('uncertainty-granularity').value;
    const granularityk = document.getElementById('k-granularity').value;

    if(kValue && !uncertaintyValue){
        uncertaintyInput.classList.add('invalid-input');
    }else if(!kValue && uncertaintyValue){
        kInput.classList.add('invalid-input');
    }else{
        kInput.classList.remove('invalid-input');
        uncertaintyInput.classList.remove('invalid-input');
    }

    removeFilterTag('uncertainty-combo');
    if (kValue && uncertaintyValue) {
        createFilterTag(
            'uncertainty-combo',
            'WNM configuration ',
            `(k: ${kValue} ${granularityk}, uncertainty: ${uncertaintyValue} ${granularityUncertainty})`,
            {"wnm-config":{granularityK:granularityk,granularityUncertainty:granularityUncertainty,
                    k:kValue,uncertaintyPerEvent:uncertaintyValue}},
            () => {
                kInput.value = '';
                uncertaintyInput.value = '';
                document.getElementById('k-granularity').value = 'seconds';
                document.getElementById('uncertainty-granularity').value = 'seconds';
                checkFilterTagVisibility();
            }
        );
    }
    //remove all tags that correspond to the pattern
    document.getElementById('filter-tag-container')
        .querySelectorAll('[id^="pattern-constraint-"]').forEach(el => el.remove());
    //create tag for each valid card
    document.getElementById('constraints-container')
        .querySelectorAll('.constraint-card')
        .forEach(card => {
            createTagFromConstraint(card);
        })
    persistFiltersToSessionStorage();
}

function createTagFromConstraint(card) {
    let valueOfA = card.querySelector('.event-a').value;
    let valueOfB = card.querySelector('.event-b').value;
    let valueOfConstraint = card.querySelector('input').value;
    const index = card.getAttribute('data-id');
    let events = getAllTagEvents();
    if (valueOfA && valueOfB && valueOfConstraint) {
        let eventA = events[valueOfA];
        let eventB = events[valueOfB];
        let toggleButtons = Array.from(card.querySelectorAll('.toggle-group .active'))
            .map(el => el.innerText.toLowerCase());
        let jsonValue={posA:parseInt(valueOfA),posB:parseInt(valueOfB), constraint:parseInt(valueOfConstraint)};

        if (toggleButtons[1] === 'time') {
            let granularity = card.querySelector('.granularity-select select').value;
            valueOfConstraint = `${valueOfConstraint} ${granularity}`;
            jsonValue.constraint_type = "timeConstraint"
            jsonValue.granularity = granularity;
        }else{
            jsonValue.constraint_type = "gapConstraint"
        }
        let value = `${eventA}(${parseInt(valueOfA)+1}) ${toggleButtons[0]} ${valueOfConstraint} from ${eventB}(${parseInt(valueOfB)+1})`;
        createFilterTag(`pattern-constraint-${index}`, "", value,
            jsonValue, () => {
            removeFilterTag(`pattern-constraint-${index}`);
            card.remove();
            checkFilterTagVisibility();
        });
    }
}

function getFilters(){
    let filters = []
    let constraints = []
    document.getElementById('filter-tag-container').querySelectorAll('.tag')
        .forEach(tag => {
            if(tag.dataset.id.includes('pattern-constraint')){
                constraints.push(JSON.parse(tag.dataset.json));
            }else{
                filters.push(JSON.parse(tag.dataset.json));
            }

    })
    filters.push({constraints:constraints});
    return filters;
}

function searchPattern(){
    // todo: implement this -> read data from filters and pattern, construct query and send it (asynced)
    // todo: container with 2 tabs (results + excel export) and stats (load pie based on the returned results)
    loadStatsFragment().then(() => {})

    console.log("Pattern Detection triggered")
    const filters = getFilters();
    const queryWrapper = {
        log_name: document.getElementById('logSelector').value,
        pattern:{eventsWithSymbols:getPattern()}
    }
    filters.forEach(filter => {
        if(filter.hasOwnProperty('from')){
            queryWrapper.from = filter.from;
        }
        if(filter.hasOwnProperty('till')){
            queryWrapper.till = filter.till;
        }
        if(filter.hasOwnProperty('returnAll')){
            queryWrapper.returnAll = filter.returnAll;
        }
        if(filter.hasOwnProperty('constraints')){
            queryWrapper.pattern.constraints = filter.constraints;
        }
        if(filter.hasOwnProperty('wnm-config')){
            queryWrapper["wnm-config"] = filter["wnm-config"];
            queryWrapper.whyNotMatchFlag = true;
        }
        if(filter.hasOwnProperty('groups-config')){
            queryWrapper["groups-config"]={groups:filter["groups-config"]}
            queryWrapper.hasGroups = true;
        }
    })



    fetch('/ui/detection', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(queryWrapper)
    })
        .then(response => response.text())
        .then(html => {
            // Load the returned fragment into the results div
            document.getElementById('pattern-results').innerHTML = html;
            initializeResultsTable("results-table");
            const resultCard = document.getElementById('pattern-results-expandable-card');
            if (resultCard) {
                resultCard.classList.remove('collapsed');
                resultCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
                resultCard.classList.add('flash-highlight');

                setTimeout(() => {
                    resultCard.classList.remove('flash-highlight');
                }, 2000); // duration must match CSS animation
            }

        })
        .catch(error => {
            console.error('Error:', error);
            document.getElementById('pattern-results').innerHTML =
                '<div class="error-message">Error loading results</div>';
        });
}

function isValidGroupDefinition(input) {
    try {
        // Step 1: Replace () with [] to make it JSON-like
        let jsonLike = input
            .replace(/\(/g, '[')
            .replace(/\)/g, ']');

        // Step 2: Use JSON5 to parse it, or use `eval` cautiously
        let groups = eval(jsonLike);

        // Step 3: Check that it's an array of arrays
        if (!Array.isArray(groups)) return false;
        return groups.every(group => Array.isArray(group));
    } catch (e) {
        return false;
    }
}
function initializeResultsTable(tableId) {
    $(`#${tableId}`).DataTable({
        destroy: true,
        pageLength: 10,
        lengthMenu: [5, 10, 25, 50],
        order: [[0, "asc"]],
        language: {
            search: "Search Trace:",
            lengthMenu: "Rows per page: _MENU_"
        }
    });
}