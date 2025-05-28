function initEventSearchBar() {
    let currentEvent = null;

    const tagsContainer = document.getElementById('tags');
    const dropdownWrapper = document.getElementById('dropdown-wrapper');
    const input = document.getElementById('event-input');
    const eventList = document.getElementById('event-list');
    const symbolList = document.getElementById('symbol-list');

    // Show event list on input click
    input.addEventListener('click', () => {
        populateEvents(events);
        dropdownWrapper.classList.remove('hidden');
    });

    // Filter while typing
    input.addEventListener('input', () => {
        const value = input.value.toLowerCase();
        const filtered = events.filter(e => e.toLowerCase().startsWith(value));
        populateEvents(filtered);
    });

    // Populate event list
    function populateEvents(list) {
        eventList.innerHTML = '';
        symbolList.classList.add('hidden');
        list.forEach(event => {
            const li = document.createElement('li');
            li.textContent = event;

            li.addEventListener('click', () => {
                currentEvent = event;
                highlightSelectedEvent(li);
                populateSymbols(symbols);
                symbolList.classList.remove('hidden');
            });

            eventList.appendChild(li);
        });
    }

    function highlightSelectedEvent(selectedLi) {
        const items = eventList.querySelectorAll('li');
        items.forEach(li => li.classList.remove('active'));
        selectedLi.classList.add('active');
    }

    // Populate symbols
    function populateSymbols(symbolListData) {
        symbolList.innerHTML = '';
        symbolListData.forEach(symbol => {
            const li = document.createElement('li');
            li.textContent = symbol;
            li.addEventListener('click', () => {
                createTag(currentEvent, symbol);
                input.value = '';
                dropdownWrapper.classList.add('hidden');
                input.focus();
            });
            symbolList.appendChild(li);
        });
    }

    // Create tag
    function createTag(event, symbol) {
        const tag = document.createElement('div');
        tag.className = 'tag';
        tag.dataset.event = event;
        tag.dataset.symbol = symbol;

        const text = document.createElement('span');
        if (symbol === '_') {
            text.textContent = `${event}`;
        } else {
            text.textContent = `${event}${symbol}`;
        }

        const closeBtn = document.createElement('span');
        closeBtn.textContent = '×';
        closeBtn.className = 'close-btn';
        closeBtn.addEventListener('click', () => {
            tag.remove();
        });

        tag.appendChild(text);
        tag.appendChild(closeBtn);
        tagsContainer.appendChild(tag);
    }

    // Hide dropdown when clicking outside
    document.addEventListener('click', (e) => {
        if (!dropdownWrapper.contains(e.target) && e.target !== input) {
            dropdownWrapper.classList.add('hidden');
        }
    });
}

function getAllTagEvents() {
    const tagsContainer = document.getElementById('tags');
    const tags = tagsContainer.querySelectorAll('.tag');
    const events = [];

    tags.forEach(tag => {
        const event = tag.dataset.event;
        events.push(event);
    });

    return events;
}
