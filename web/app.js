const boardEl = document.getElementById('board');
const statusEl = document.getElementById('status');
const turnEl = document.getElementById('turn');
const timerEl = document.getElementById('timer');
const leaderboardEl = document.getElementById('leaderboard');
const joinButton = document.getElementById('join');
const usernameInput = document.getElementById('username');
const gameSection = document.getElementById('game');
const resetButton = document.getElementById('reset');

let socket;
let currentState;
let username = '';
let countdownInterval;
let shouldReconnect = true;

function buildBoard(board) {
  boardEl.innerHTML = '';
  board.forEach((row, rowIndex) => {
    row.forEach((cell, colIndex) => {
      const div = document.createElement('div');
      div.className = 'cell';
      if (cell === 1) div.classList.add('player1');
      if (cell === 2) div.classList.add('player2');
      div.addEventListener('click', () => sendMove(colIndex));
      boardEl.appendChild(div);
    });
  });
}

function updateState(state) {
  currentState = state;
  buildBoard(state.board);
  if (state.status === 'finished') {
    if (state.winner) {
      statusEl.textContent = `Winner: ${state.winner}`;
    } else {
      statusEl.textContent = 'Game ended in a draw.';
    }
    shouldReconnect = false;
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.close();
    }
  } else if (state.status === 'waiting') {
    statusEl.textContent = 'Waiting for opponent or bot...';
  } else {
    statusEl.textContent = state.message || '';
  }
  turnEl.textContent = state.turn ? `Turn: ${state.turn}` : '';
  updateTimer();
}

function updateTimer() {
  if (countdownInterval) {
    clearInterval(countdownInterval);
  }
  if (!currentState || currentState.status !== 'active' || !currentState.turnStartedAt) {
    timerEl.textContent = '';
    return;
  }
  const turnStarted = new Date(currentState.turnStartedAt).getTime();
  countdownInterval = setInterval(() => {
    const elapsed = Math.floor((Date.now() - turnStarted) / 1000);
    const remaining = Math.max(0, 30 - elapsed);
    timerEl.textContent = `Time left: ${remaining}s`;
    if (remaining === 0) {
      clearInterval(countdownInterval);
    }
  }, 500);
}

function sendMove(col) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  if (!currentState || currentState.status !== 'active') return;
  if (currentState.turn !== username) return;
  socket.send(JSON.stringify({ type: 'move', column: col }));
}

function connect() {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.close();
  }
  socket = new WebSocket(`ws://${window.location.host}/ws`);
  socket.addEventListener('open', () => {
    socket.send(JSON.stringify({ type: 'join', username }));
  });
  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'state') {
      updateState(data);
      refreshLeaderboard();
    } else if (data.type === 'status') {
      statusEl.textContent = data.message;
    } else if (data.type === 'error') {
      statusEl.textContent = data.message;
    }
  });
  socket.addEventListener('close', () => {
    if (!shouldReconnect) {
      statusEl.textContent = 'Disconnected. Click Join or Play Again to start a new game.';
      return;
    }
    statusEl.textContent = 'Disconnected. Attempting to reconnect...';
    setTimeout(connect, 1000);
  });
}

function refreshLeaderboard() {
  fetch('/api/leaderboard')
    .then((res) => res.json())
    .then((leaders) => {
      leaderboardEl.innerHTML = '';
      leaders.forEach((leader) => {
        const li = document.createElement('li');
        li.textContent = `${leader.username}: ${leader.wins}`;
        leaderboardEl.appendChild(li);
      });
    })
    .catch(() => {});
}

function startSession() {
  const requested = usernameInput.value.trim();
  if (requested) {
    username = requested;
  }
  if (!username) {
    alert('Username required');
    return;
  }
  shouldReconnect = true;
  gameSection.style.display = 'block';
  connect();
}

joinButton.addEventListener('click', () => {
  startSession();
});

resetButton.addEventListener('click', () => {
  startSession();
});
