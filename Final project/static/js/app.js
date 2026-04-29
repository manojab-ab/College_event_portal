const filterSelect = document.getElementById("departmentFilter");
const eventCards = document.querySelectorAll("#eventGrid .event-card");
const passwordToggles = document.querySelectorAll(".password-toggle");
const addCompetitionButton = document.getElementById("addCompetitionButton");
const competitionStack = document.getElementById("competitionStack");
const schoolSelects = document.querySelectorAll("[data-school-select]");
const panelButtons = document.querySelectorAll("[data-panel-target]");
const managerPanels = document.querySelectorAll(".manager-panel");
const competitionSelect = document.getElementById("competition_id");
const addTeamMemberButton = document.getElementById("addTeamMemberButton");
const teamMemberStack = document.getElementById("teamMemberStack");
const teamMemberLimitText = document.getElementById("teamMemberLimitText");
const winnerEventSelect = document.getElementById("winner_event_name");
const winnerCompetitionSelect = document.getElementById("winner_competition_name");
const winnerCompetitionMapNode = document.getElementById("winnerCompetitionMap");
const visualEventSelect = document.getElementById("visual_event_name");
const visualCompetitionSelect = document.getElementById("visual_competition_name");
const visualCompetitionMapNode = document.getElementById("visualCompetitionMap");
const announceCompetitionSelect = document.getElementById("announce_competition_id");
const announcePositionSelect = document.getElementById("announce_position");
const announceResultLabelSelect = document.getElementById("announce_result_label");
const announcePrizeInput = document.getElementById("announce_prize_money");
const announceRegistrationInput = document.getElementById("announce_registration_number");
const announceParticipantInput = document.getElementById("announce_participant_name");
const announceTeamInput = document.getElementById("announce_team_name");
const eventRegistrationLookupNode = document.getElementById("eventRegistrationLookup");

if (filterSelect) {
    filterSelect.addEventListener("change", (event) => {
        const selected = event.target.value;

        eventCards.forEach((card) => {
            const departments = (card.dataset.departments || "").split(",");
            const shouldShow = selected === "all" || departments.includes(selected);
            card.classList.toggle("hidden", !shouldShow);
        });
    });
}

passwordToggles.forEach((toggle) => {
    toggle.addEventListener("change", () => {
        const targets = (toggle.dataset.target || "").split(",");
        targets.forEach((targetId) => {
            const field = document.getElementById(targetId.trim());
            if (field) {
                field.type = toggle.checked ? "text" : "password";
            }
        });
    });
});

function syncDepartmentSelect(schoolSelect) {
    const targetId = schoolSelect.dataset.departmentTarget;
    if (!targetId) return;
    const departmentSelect = document.getElementById(targetId);
    if (!departmentSelect) return;
    const schoolId = schoolSelect.value;
    const options = departmentSelect.querySelectorAll("option[data-school-id]");
    options.forEach((option) => {
        const shouldShow = !schoolId || option.dataset.schoolId === schoolId;
        option.hidden = !shouldShow;
    });
    const selectedOption = departmentSelect.options[departmentSelect.selectedIndex];
    if (selectedOption && selectedOption.hidden) {
        departmentSelect.value = "";
    }
}

schoolSelects.forEach((select) => {
    syncDepartmentSelect(select);
    select.addEventListener("change", () => syncDepartmentSelect(select));
});

if (panelButtons.length && managerPanels.length) {
    panelButtons.forEach((button) => {
        button.addEventListener("click", () => {
            const targetId = button.dataset.panelTarget;
            managerPanels.forEach((panel) => {
                panel.hidden = panel.id !== targetId;
            });
        });
    });
}

if (addCompetitionButton && competitionStack) {
    addCompetitionButton.addEventListener("click", () => {
        const index = competitionStack.querySelectorAll("[data-competition-card]").length + 1;
        const card = document.createElement("div");
        card.className = "competition-card";
        card.dataset.competitionCard = "true";
        card.innerHTML = `
            <strong>Competition ${index}</strong>
            <div class="form-grid">
                <input name="competition_name_${index}" type="text" placeholder="Competition name">
                <input name="competition_venue_${index}" type="text" placeholder="Competition venue">
                <input name="max_team_members_${index}" type="number" min="1" value="1" placeholder="Max team members">
                <input name="first_prize_${index}" type="number" placeholder="1st prize">
                <input name="second_prize_${index}" type="number" placeholder="2nd prize">
                <input name="third_prize_${index}" type="number" placeholder="3rd prize">
            </div>
        `;
        competitionStack.appendChild(card);
    });
}

function getSelectedCompetitionMax() {
    if (!competitionSelect) return 1;
    const selectedOption = competitionSelect.options[competitionSelect.selectedIndex];
    const max = Number(selectedOption?.dataset.maxTeamMembers || "1");
    return Number.isFinite(max) && max > 0 ? max : 1;
}

function updateTeamMemberState() {
    if (!teamMemberStack || !teamMemberLimitText || !addTeamMemberButton) return;
    const maxMembers = getSelectedCompetitionMax();
    while (teamMemberStack.querySelectorAll('input[name="team_member_name"]').length > maxMembers) {
        teamMemberStack.removeChild(teamMemberStack.lastElementChild);
    }
    const currentCount = teamMemberStack.querySelectorAll('input[name="team_member_name"]').length;
    teamMemberLimitText.textContent = `Maximum team members allowed for this competition: ${maxMembers}`;
    addTeamMemberButton.disabled = currentCount >= maxMembers;
}

function addTeamMemberInput() {
    if (!teamMemberStack) return;
    const maxMembers = getSelectedCompetitionMax();
    const currentInputs = teamMemberStack.querySelectorAll('input[name="team_member_name"]');
    if (currentInputs.length >= maxMembers) {
        updateTeamMemberState();
        return;
    }

    const nextIndex = currentInputs.length + 1;
    const input = document.createElement("input");
    input.name = "team_member_name";
    input.type = "text";
    input.placeholder = `Team member ${nextIndex}`;
    teamMemberStack.appendChild(input);
    updateTeamMemberState();
}

if (competitionSelect && teamMemberStack && addTeamMemberButton) {
    competitionSelect.addEventListener("change", updateTeamMemberState);
    addTeamMemberButton.addEventListener("click", addTeamMemberInput);
    updateTeamMemberState();
}

if (winnerEventSelect && winnerCompetitionSelect && winnerCompetitionMapNode) {
    const winnerCompetitionMap = JSON.parse(winnerCompetitionMapNode.textContent || "{}");

    function renderWinnerCompetitionOptions(resetSelection = false) {
        const eventName = winnerEventSelect.value;
        const competitionNames = winnerCompetitionMap[eventName] || [];
        const selectedValue = resetSelection ? "" : winnerCompetitionSelect.value;

        winnerCompetitionSelect.innerHTML = "";
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = eventName
            ? (competitionNames.length ? "Select competition" : "No competitions under this event")
            : "Select event first";
        winnerCompetitionSelect.appendChild(placeholder);

        competitionNames.forEach((competitionName) => {
            const option = document.createElement("option");
            option.value = competitionName;
            option.textContent = competitionName;
            if (competitionName === selectedValue) {
                option.selected = true;
            }
            winnerCompetitionSelect.appendChild(option);
        });
    }

    winnerEventSelect.addEventListener("change", () => renderWinnerCompetitionOptions(true));
    renderWinnerCompetitionOptions(false);
}

if (visualEventSelect && visualCompetitionSelect && visualCompetitionMapNode) {
    const visualCompetitionMap = JSON.parse(visualCompetitionMapNode.textContent || "{}");

    function renderVisualCompetitionOptions(resetSelection = false) {
        const eventName = visualEventSelect.value;
        const competitionNames = visualCompetitionMap[eventName] || [];
        const selectedValue = resetSelection ? "" : visualCompetitionSelect.value;

        visualCompetitionSelect.innerHTML = "";
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = eventName
            ? (competitionNames.length ? "Select competition" : "No competitions under this event")
            : "All competitions";
        visualCompetitionSelect.appendChild(placeholder);

        competitionNames.forEach((competitionName) => {
            const option = document.createElement("option");
            option.value = competitionName;
            option.textContent = competitionName;
            if (competitionName === selectedValue) {
                option.selected = true;
            }
            visualCompetitionSelect.appendChild(option);
        });
    }

    visualEventSelect.addEventListener("change", () => renderVisualCompetitionOptions(true));
    renderVisualCompetitionOptions(false);
}

if (
    announceCompetitionSelect &&
    announcePositionSelect &&
    announceResultLabelSelect &&
    announcePrizeInput &&
    announceRegistrationInput &&
    announceParticipantInput &&
    announceTeamInput &&
    eventRegistrationLookupNode
) {
    const registrationRows = JSON.parse(eventRegistrationLookupNode.textContent || "[]");

    function syncAnnouncementPrize() {
        const option = announceCompetitionSelect.options[announceCompetitionSelect.selectedIndex];
        const prizeMap = {
            "1": option?.dataset.firstPrize || "0",
            "2": option?.dataset.secondPrize || "0",
            "3": option?.dataset.thirdPrize || "0",
        };
        announcePrizeInput.value = prizeMap[announcePositionSelect.value] || "";
    }

    function syncPositionAndLabel(source) {
        const labelMap = {
            "1": "Winner",
            "2": "First Runner-up",
            "3": "Second Runner-up",
        };
        const positionMap = {
            "Winner": "1",
            "First Runner-up": "2",
            "Second Runner-up": "3",
        };
        if (source === "position" && announcePositionSelect.value) {
            announceResultLabelSelect.value = labelMap[announcePositionSelect.value] || "";
        }
        if (source === "label" && announceResultLabelSelect.value) {
            announcePositionSelect.value = positionMap[announceResultLabelSelect.value] || "";
        }
        syncAnnouncementPrize();
    }

    function syncAnnouncementParticipant() {
        const entered = (announceRegistrationInput.value || "").trim().toUpperCase();
        const match = registrationRows.find(
            (row) => String(row.registration_number || "").trim().toUpperCase() === entered
        );
        if (!match) {
            return;
        }
        announceParticipantInput.value = match.participant_name || match.account_name || "";
        announceTeamInput.value = match.team_name || "";
    }

    announceCompetitionSelect.addEventListener("change", syncAnnouncementPrize);
    announcePositionSelect.addEventListener("change", () => syncPositionAndLabel("position"));
    announceResultLabelSelect.addEventListener("change", () => syncPositionAndLabel("label"));
    announceRegistrationInput.addEventListener("input", syncAnnouncementParticipant);
    syncAnnouncementPrize();
    syncAnnouncementParticipant();
}
