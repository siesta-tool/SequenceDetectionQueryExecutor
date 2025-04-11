
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

document.addEventListener('DOMContentLoaded', function () {
    loadSidebar("");
});

function loadSidebar(logname) {
    fetch(`/fragments/sidebar-tabs?currentLogname=${logname}`)
        .then(res => res.text())
        .then(html => {
            document.getElementById("sidebar-tabs").innerHTML = html;
        });
}

function processTab(){
    window.tabOpen = "process-tab";
    document.querySelectorAll('.tab')
        .forEach(tab => tab.classList.remove('active-tab'));
    //set active tab to preprocess
    document.getElementById("process-tab").classList.add('active-tab')
    document.getElementById("navbar-title").textContent = 'Preprocess';
    //pending to open page in the middle
    document.getElementById("main-panel").innerHTML = "";
}

function patternDetectionTab(){
    window.tabOpen = "detection-tab";
    document.querySelectorAll('.tab')
        .forEach(tab => tab.classList.remove('active-tab'));
    //set active tab to preprocess
    document.getElementById("detection-tab").classList.add('active-tab')
    document.getElementById("navbar-title").textContent = 'Pattern Detection';


    fetch(`/fragments/pattern-detection`)
        .then(res => res.text())
        .then(html => {
            const container = document.getElementById("main-panel");
            container.innerHTML = html;
            if (typeof componentHandler !== 'undefined') {
                componentHandler.upgradeDom();
            }

            // Dynamically import the JS module and initialize
            import('/js/components/event_search_bar.js')
                .then(module => {
                    module.initEventSearchBar(); // safe, scoped, repeatable
                })
                .catch(err => console.error('Failed to load module:', err));


        })
        .catch(err => {
            console.error("Failed to load pattern detection", err);
        });

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