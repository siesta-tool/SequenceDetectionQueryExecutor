function startRefresh() {
    const btn = document.getElementById("refresh-btn");
    const spinner = document.getElementById("spinner");

    // Hide button, show spinner
    btn.style.display = "none";
    spinner.classList.add("is-active");

    fetch('/refreshData')
        .then(() => {
            return fetch(`/fragments/sidebar-tabs?currentLogname=${currentLogname}`);
        })
        .then(res => res.text())
        .then(html => {
            document.getElementById("sidebar-tabs").innerHTML = html;
        })
        .catch(error => {
            console.error("Error during refresh:", error);
            alert("Failed to refresh data.");
        })
        .finally(() => {
            spinner.classList.remove("is-active");
            btn.style.display = "inline-block";
        });
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

function metadataLog(logname) {
    window.tabOpen = logname;

    // Optional: highlight active tab manually if not server-rendered
    document.querySelectorAll('.tab')
        .forEach(tab => tab.classList.remove('active-tab'));

    // This assumes the clicked tab had unique logname text
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
            // componentHandler.upgradeDom();
            // const selector = container.querySelector("#graph-selector");
            // if (selector) {
            //     selector.addEventListener("change", window.handleGraphSelection);
            // }
            // const selector2 = container.querySelector("#eventList");
            // if (selector2) {
            //     selector2.addEventListener("change", window.updateEventInfo);
            // }
        })
        .catch(err => {
            console.error("Failed to load metadata", err);
        });
}