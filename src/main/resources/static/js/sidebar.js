function fetchLognamesAndRender() {
    fetch('/lognames')
        .then(res => res.json())
        .then(data => {
            const container = document.getElementById('logname-links');
            container.innerHTML = ''; // Clear previous content if needed
            const currentPath = window.location.pathname;

            data.forEach(logname => {
                const link = document.createElement('a');
                link.className = 'link';
                const href = `/ui/query/${logname}`;
                link.href = href;
                if (currentPath === href) {
                    link.classList.add('active-log'); // Add highlight if it's the current log
                }
                link.innerHTML = `
                  <i class="material-icons" style="color: #3F51B5;">table_chart</i>
                  <span>${logname}</span>
                `;
                container.appendChild(link);
            });
        })
        .catch(err => {
            console.error("Error fetching lognames:", err);
        });
}

function startRefresh() {
    const btn = document.getElementById("refresh-btn");
    const spinner = document.getElementById("spinner");

    // Hide button, show spinner
    btn.style.display = "none";
    spinner.classList.add("is-active");

    fetch('/refreshData')
        .then(response => {
            if (!response.ok) {
                throw new Error("Refresh failed");
            }
            return fetchLognamesAndRender();
        })
        .catch(error => {
            console.error("Error during refresh:", error);
            alert("Failed to refresh data.");
        })
        .finally(() => {
            // Stop spinner, show button again
            spinner.classList.remove("is-active");
            btn.style.display = "inline-block";
        });
}

document.addEventListener('DOMContentLoaded', function () {
    fetchLognamesAndRender();
});