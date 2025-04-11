function togglePanel(header) {
    const card = header.closest('.expandable-card');
    card.classList.toggle('collapsed');
    header.querySelector('.icon-down').classList.toggle('hidden')
    header.querySelector('.icon-up').classList.toggle('hidden')
}

