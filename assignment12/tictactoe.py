class TictactoeException(Exception):
    """
    Custom exception class for tic-tac-toe game errors.
    
    This class demonstrates how to create specialized exceptions that provide
    more meaningful error messages than generic Python exceptions. Instead of
    getting a confusing system error, players receive clear, game-specific
    feedback about what went wrong.
    
    Think of this as creating a specialized error reporting system that speaks
    the language of our game domain.
    """
    
    def __init__(self, message):
        """
        Initialize our custom exception with a specific error message.
        
        This method demonstrates the proper way to extend built-in classes
        while preserving their original functionality. We store our custom
        message and then call the parent class constructor to ensure all
        the standard exception machinery works correctly.
        
        Args:
            message (str): The specific error message to display to the user
        """
        # Store our custom message as an instance variable
        self.message = message
        
        # Call the parent Exception class constructor
        # This ensures our exception works properly with Python's error handling system
        super().__init__(message)



class Board:
    """
    Represents a tic-tac-toe game board with complete game logic.
    
    This class encapsulates all the game state, move validation, win detection,
    and display logic. It demonstrates how object-oriented design can create
    clean, maintainable code by grouping related functionality together.
    
    Think of this class as both the game board and the referee - it knows
    the current state of the game and enforces all the rules.
    """
    
    # Class variable defining all valid move positions
    # This is shared by all instances of the Board class
    valid_moves = [
        "upper left", "upper center", "upper right",
        "middle left", "center", "middle right", 
        "lower left", "lower center", "lower right"
    ]
    
    def __init__(self):
        """
        Initialize a new tic-tac-toe board.
        
        This method sets up the initial game state with an empty 3x3 grid
        and establishes that X goes first (following traditional tic-tac-toe rules).
        
        The board is represented as a list of lists, creating a 2D grid that
        maps naturally to the visual layout of a tic-tac-toe board.
        """
        # Create a 3x3 grid filled with empty spaces
        # Each sub-list represents a row of the board
        self.board_array = [
            [" ", " ", " "],  # Top row
            [" ", " ", " "],  # Middle row  
            [" ", " ", " "]   # Bottom row
        ]
        
        # Track whose turn it is (X always goes first in tic-tac-toe)
        self.turn = "X"



    def __str__(self):
        """
        Convert the board into a visually appealing string representation.
        
        This method demonstrates how to transform internal data structures
        into user-friendly displays. It creates a classic tic-tac-toe grid
        with proper formatting that's immediately recognizable to players.
        
        Returns:
            str: A formatted string showing the current board state
        """
        lines = []
        
        # Build the visual representation row by row
        # Each row shows the three positions separated by vertical bars
        lines.append(f" {self.board_array[0][0]} | {self.board_array[0][1]} | {self.board_array[0][2]} \n")
        lines.append("-----------\n")
        lines.append(f" {self.board_array[1][0]} | {self.board_array[1][1]} | {self.board_array[1][2]} \n")
        lines.append("-----------\n")
        lines.append(f" {self.board_array[2][0]} | {self.board_array[2][1]} | {self.board_array[2][2]} \n")
        
        # Join all the lines into a single string
        return "".join(lines)


    def move(self, move_string):
        """
        Process a player's move with comprehensive validation.
        
        This method demonstrates robust input validation and state management.
        It checks that the move is valid, the position is available, updates
        the board state, and switches turns - all while providing clear error
        messages if anything goes wrong.
        
        Args:
            move_string (str): The position where the player wants to move
            
        Raises:
            TictactoeException: If the move is invalid or the position is taken
        """
        # First validation: Is this a recognized move?
        if move_string not in Board.valid_moves:
            raise TictactoeException("That's not a valid move.")
        
        # Convert the move string to board coordinates
        # This demonstrates how to map user-friendly names to internal indices
        move_index = Board.valid_moves.index(move_string)
        row = move_index // 3      # Integer division gives us the row
        column = move_index % 3    # Modulo gives us the column
        
        # Second validation: Is this position already occupied?
        if self.board_array[row][column] != " ":
            raise TictactoeException("That spot is taken.")
        
        # Execute the move: place the current player's mark
        self.board_array[row][column] = self.turn
        
        # Switch to the other player for the next turn
        if self.turn == "X":
            self.turn = "O"
        else:
            self.turn = "X"


    def whats_next(self):
        """
        Analyze the current board state to determine if the game is over.
        
        This method demonstrates comprehensive state analysis, checking for
        all possible win conditions and determining the appropriate next action.
        It returns structured data that the main game loop can use to decide
        how to proceed.
        
        Returns:
            tuple: (game_is_over: bool, status_message: str)
        """
        # First, check if the board is full (potential cat's game)
        cat = True
        for i in range(3):
            for j in range(3):
                if self.board_array[i][j] == " ":
                    cat = False
                    break  # Exit inner loop
            else:
                continue  # Continue outer loop if inner loop completed normally
            break  # Exit outer loop if we found an empty space
        
        # If board is full and we haven't found a winner yet, it's a cat's game
        if cat:
            return (True, "Cat's Game.")
        
        # Check for winning conditions
        win = False
        
        # Check all rows for three in a row
        for i in range(3):
            if self.board_array[i][0] != " ":  # Row has content
                if (self.board_array[i][0] == self.board_array[i][1] and 
                    self.board_array[i][1] == self.board_array[i][2]):
                    win = True
                    break
        
        # Check all columns for three in a row (if no row winner found)
        if not win:
            for i in range(3):
                if self.board_array[0][i] != " ":  # Column has content
                    if (self.board_array[0][i] == self.board_array[1][i] and 
                        self.board_array[1][i] == self.board_array[2][i]):
                        win = True
                        break
        
        # Check diagonals for three in a row (if no other winner found)
        if not win:
            if self.board_array[1][1] != " ":  # Center position has content
                # Check main diagonal (top-left to bottom-right)
                if (self.board_array[0][0] == self.board_array[1][1] and 
                    self.board_array[2][2] == self.board_array[1][1]):
                    win = True
                # Check anti-diagonal (top-right to bottom-left)
                if (self.board_array[0][2] == self.board_array[1][1] and 
                    self.board_array[2][0] == self.board_array[1][1]):
                    win = True
        
        # Determine the appropriate return value based on our analysis
        if not win:
            # Game continues - indicate whose turn it is
            if self.turn == "X":
                return (False, "X's turn.")
            else:
                return (False, "O's turn.")
        else:
            # Someone won - determine who based on whose turn it is now
            # (Remember: we switched turns after the winning move)
            if self.turn == "O":
                return (True, "X wins!")
            else:
                return (True, "O wins!")


def play_tic_tac_toe():
    """
    Main game loop that coordinates the entire tic-tac-toe playing experience.
    
    This function demonstrates how to create engaging user interfaces while
    handling all the edge cases and error conditions that can arise during
    interactive gameplay.
    """
    print("🎮 Welcome to Tic-Tac-Toe! 🎮")
    print("=" * 40)
    print("Move positions:")
    print("upper left    | upper center  | upper right")
    print("middle left   | center        | middle right") 
    print("lower left    | lower center  | lower right")
    print("=" * 40)
    
    # Create a new game board
    board = Board()
    move_count = 0
    
    # Main game loop - continues until the game ends
    while True:
        # Display current board state
        print(f"\nMove #{move_count + 1}")
        print(board)
        
        # Check if the game is over
        game_over, status = board.whats_next()
        
        if game_over:
            print(f"🎉 Game Over: {status}")
            if "wins" in status:
                print("Congratulations to the winner!")
            else:
                print("Good game - it was a tie!")
            break
        
        # Get the next move from the current player
        print(f"It's {board.turn}'s turn.")
        
        try:
            # Get player input with validation
            move = input("Enter your move: ").strip().lower()
            
            # Handle quit condition
            if move in ['quit', 'exit', 'q']:
                print("Thanks for playing!")
                break
            
            # Attempt to make the move
            board.move(move)
            move_count += 1
            
        except TictactoeException as e:
            # Handle game-specific errors with helpful messages
            print(f"❌ {e.message}")
            print("Please try again.")
            
        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            print("\n\nGame interrupted. Thanks for playing!")
            break
        
        except Exception as e:
            # Handle any unexpected errors
            print(f"❌ An unexpected error occurred: {e}")
            print("Please try again.")

def main():
    """
    Main program entry point with game session management.
    """
    print("🎲 Advanced Python: Object-Oriented Game Development 🎲")
    print("This tic-tac-toe game demonstrates:")
    print("• Custom exception classes for domain-specific error handling")
    print("• Comprehensive class design with encapsulated state and behavior") 
    print("• Robust user input validation and error recovery")
    print("• Clean separation between game logic and user interface")
    
    while True:
        play_tic_tac_toe()
        
        # Ask if they want to play again
        play_again = input("\nWould you like to play another game? (y/n): ").strip().lower()
        if play_again not in ['y', 'yes']:
            print("Thanks for playing! Hope you enjoyed learning about OOP! 🚀")
            break

# Execute the game when the script is run directly
if __name__ == "__main__":
    main()            

                        