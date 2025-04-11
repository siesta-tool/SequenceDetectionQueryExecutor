function startRefreshButton(container) {
    const btn = container.querySelector('.refresh-btn');
    const spinner = container.querySelector('.spinner');
    btn.classList.add("hidden");
    spinner.classList.add("is-active");
}

function stopRefreshButton(container) {
    const btn = container.querySelector('.refresh-btn');
    const spinner = container.querySelector('.spinner');
    spinner.classList.remove("is-active");
    btn.classList.remove("hidden");
}

function refreshButtonFunction(container, functionName) {
    console.log("refreshing button")
    startRefreshButton(container);

    // Safely call user-defined function (async supported)
    const fn = window[functionName];
    if (typeof fn === 'function') {
        const result = fn();

        // If it's a Promise, wait for it
        if (result && typeof result.then === 'function') {
            result.finally(() => stopRefreshButton(container));
        } else {
            stopRefreshButton(container);
        }
    } else {
        console.warn(`Function '${functionName}' is not defined.`);
        stopRefreshButton(container);
    }
}