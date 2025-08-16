"""
Fast approximation algorithm for healthcare project selection.
Uses a greedy approach based on cost-effectiveness ratios.
"""

from dataclasses import dataclass
from typing import List, Dict
import time
import numpy as np



class Project:
    def __init__(self, id, cost, qaly):
        self.id = id
        self.cost = cost
        self.qaly = qaly


@dataclass
class ApproxSolution:
    """Represents a solution from the approximation algorithm."""
    selected_projects: List[Project]
    total_cost: float
    total_qaly: float
    cost_effectiveness: float
    computation_time: float


def greedy_ratio_approximation(projects: List[Project], budget: float) -> ApproxSolution:
    """
    Fast greedy approximation algorithm that selects projects based on their QALY/cost ratio.

    Args:
        projects: List of healthcare projects
        budget: Maximum budget constraint

    Returns:
        ApproxSolution with the selected projects
    """
    start_time = time.time()

    # Calculate QALY/cost ratio for each project
    project_ratios = [(p, p.qaly / p.cost) for p in projects]

    # Sort projects by decreasing ratio (most cost-effective first)
    sorted_projects = sorted(project_ratios, key=lambda x: x[1], reverse=True)

    # Greedy selection
    selected_projects = []
    remaining_budget = budget
    total_qaly = 0.0

    for project, ratio in sorted_projects:
        if project.cost <= remaining_budget:
            selected_projects.append(project)
            remaining_budget -= project.cost
            total_qaly += project.qaly

    # Calculate total cost
    total_cost = budget - remaining_budget

    # Calculate cost-effectiveness
    cost_effectiveness = total_qaly / total_cost if total_cost > 0 else 0

    # Calculate computation time
    computation_time = time.time() - start_time

    return ApproxSolution(
        selected_projects=selected_projects,
        total_cost=total_cost,
        total_qaly=total_qaly,
        cost_effectiveness=cost_effectiveness,
        computation_time=computation_time
    )


def local_search_improvement(initial_solution: ApproxSolution, projects: List[Project],
                            budget: float, max_iterations: int = 10000) -> ApproxSolution:
    """
    Local search algorithm to improve an initial greedy solution.
    Tries to swap projects to improve the total QALY.

    Args:
        initial_solution: Initial solution to improve
        projects: List of all projects
        budget: Maximum budget constraint
        max_iterations: Maximum number of iterations

    Returns:
        Improved ApproxSolution
    """
    start_time = time.time()

    # Start with the initial solution
    current_solution = initial_solution.selected_projects.copy()
    current_cost = initial_solution.total_cost
    current_qaly = initial_solution.total_qaly

    # Set of projects not in the solution
    excluded_projects = [p for p in projects if p not in current_solution]

    # Try to improve the solution
    improved = True
    iteration = 0

    while improved and iteration < max_iterations:
        improved = False
        iteration += 1

        # Try swapping each excluded project with each included project
        for excluded in excluded_projects:
            for i, included in enumerate(current_solution):
                # Check if swap is feasible
                new_cost = current_cost - included.cost + excluded.cost

                if new_cost <= budget:
                    # Calculate new QALY
                    new_qaly = current_qaly - included.qaly + excluded.qaly

                    # If improvement found, make the swap
                    if new_qaly > current_qaly:
                        current_solution[i] = excluded
                        excluded_projects.remove(excluded)
                        excluded_projects.append(included)
                        current_cost = new_cost
                        current_qaly = new_qaly
                        improved = True
                        break

            if improved:
                break

    # Calculate computation time including the initial solution time
    computation_time = time.time() - start_time + initial_solution.computation_time

    return ApproxSolution(
        selected_projects=current_solution,
        total_cost=current_cost,
        total_qaly=current_qaly,
        cost_effectiveness=current_qaly / current_cost if current_cost > 0 else 0,
        computation_time=computation_time
    )


def analyze_budget_levels_fast(projects: List[Project], max_budget: float,
                              num_levels: int = 20) -> Dict[float, ApproxSolution]:
    """
    Analyze multiple budget levels using the fast approximation algorithm.

    Args:
        projects: List of healthcare projects
        max_budget: Maximum budget to consider
        num_levels: Number of budget levels to analyze

    Returns:
        Dictionary mapping budget levels to solutions
    """
    # Generate budget levels
    budget_levels = np.linspace(min(p.cost for p in projects), max_budget, num_levels)

    # Solve for each budget level
    solutions = {}

    for budget in budget_levels:
        # Use greedy approximation
        solution = greedy_ratio_approximation(projects, budget)

        # Improve with local search
        improved_solution = local_search_improvement(solution, projects, budget)

        solutions[budget] = improved_solution

    return solutions


def rank_projects_by_inclusion(budget_solutions: Dict[float, ApproxSolution]) -> Dict[str, Dict]:
    """
    Rank projects by how often they appear in optimal solutions across budget levels.
    Only considers full inclusion of projects (100% of project cost).

    Args:
        budget_solutions: Dictionary mapping budget levels to optimal solutions

    Returns:
        Dictionary with project rankings and statistics
    """
    # Initialize counters
    project_stats = {}
    total_budgets = len(budget_solutions)

    # Count inclusions and track first inclusion
    for budget, solution in sorted(budget_solutions.items()):
        for project in solution.selected_projects:
            if project.id not in project_stats:
                project_stats[project.id] = {
                    "inclusion_count": 0,
                    "first_included_at": float('inf'),
                    "cost": project.cost,
                    "qaly": project.qaly
                }

            project_stats[project.id]["inclusion_count"] += 1
            project_stats[project.id]["first_included_at"] = min(
                project_stats[project.id]["first_included_at"],
                budget
            )

    # Calculate frequency of selection across budget levels
    for id, stats in project_stats.items():
        stats["selection_frequency"] = (stats["inclusion_count"] / total_budgets) * 100

    # Sort by selection frequency (descending) and first included at (ascending)
    sorted_stats = dict(sorted(
        project_stats.items(),
        key=lambda x: (-x[1]["selection_frequency"], x[1]["first_included_at"])
    ))

    return sorted_stats


def get_ranks(projects: List[Dict]) -> Dict:
    projects = [Project(p['id'], p['cost'], p['qaly']) for p in projects]

    # Set budget for comparison equal to the total cost of all projects
    budget = sum(p.cost for p in projects)

    budget_solutions = analyze_budget_levels_fast(projects, max_budget=budget, num_levels=100)
    inclusion_rankings = rank_projects_by_inclusion(budget_solutions)

    return dict(enumerate(inclusion_rankings.keys(), 1))
