
function refreshMetadata() {
    return fetch('/refreshData')
        .then(() => {
            return fetch(`/fragments/sidebar-tabs`)
        })
        .then(res => res.text())
        .then(html => {
            document.getElementById("sidebar-tabs").innerHTML = html;
        })
        .catch(error => {
            console.error("Error during refresh:", error);
            alert("Failed to refresh data.");
        })
}

function loadSidebar(logname, callback) {
    fetch(`/fragments/sidebar-tabs?currentLogname=${logname}`)
        .then(res => res.text())
        .then(html => {
            document.getElementById("sidebar-tabs").innerHTML = html;
            if (typeof callback === 'function') {
                callback();
            }
        })
        .catch(err => console.error("Failed to load sidebar:", err));
}

function processTab(push = true){
    window.tabOpen = "process-tab";
    document.querySelectorAll('.tab')
        .forEach(tab => tab.classList.remove('active-tab'));
    //set active tab to preprocess
    document.getElementById("process-tab").classList.add('active-tab')
    document.getElementById("navbar-title").textContent = 'Preprocess';

    if (push) {
        history.pushState({ tab: 'preprocess' }, '', '/siesta/preprocess');
    }

    //pending to open page in the middle
    document.getElementById("main-panel").innerHTML = "";
}

function patternDetectionTab(push = true) {
    window.tabOpen = "detection-tab";

    document.querySelectorAll('.tab')
        .forEach(tab => tab.classList.remove('active-tab'));

    document.getElementById("detection-tab").classList.add('active-tab');
    document.getElementById("navbar-title").textContent = 'Pattern Detection';

    if (push) {
        history.pushState({ tab: 'pattern-detection' }, '', '/siesta/pattern-detection');
    }

    fetch(`/fragments/pattern-detection`)
        .then(res => res.text())
        .then(html => {
            const container = document.getElementById("main-panel");
            container.innerHTML = html;

            if (typeof componentHandler !== 'undefined') {
                componentHandler.upgradeDom();
            }

            import('/js/components/event_search_bar.js')
                .then(() => {
                    initEventSearchBar();
                })
                .catch(err => console.error('Failed to load module:', err));
        })
        .catch(err => {
            console.error("Failed to load pattern detection", err);
        });
}

function navigateToMetadata(logname) {
    history.pushState({ tab: 'metadata', logname }, '', `/siesta/metadata?logname=${encodeURIComponent(logname)}`);
    metadataLog(logname);
}

function metadataLog(logname) {
    window.tabOpen = logname;

    // Optional: highlight active tab manually if not server-rendered
    document.querySelectorAll('.tab')
        .forEach(tab => tab.classList.remove('active-tab'));

    const clickedTab = Array.from(document.querySelectorAll('.tab')).find(t =>
        t.innerText.includes(logname)
    );
    if (clickedTab) clickedTab.classList.add('active-tab');

    document.getElementById("navbar-title").textContent = 'Metadata: '+logname;

    // Fetch and inject metadata panel
    fetch(`/fragments/metadata-panel?logname=${logname}`)
        .then(res => res.text())
        .then(html => {
            const container = document.getElementById("main-panel");
            container.innerHTML = html;
        })
        .catch(err => {
            console.error("Failed to load metadata", err);
        });
}