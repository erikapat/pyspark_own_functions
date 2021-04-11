
#dependencies of the function
from dateutil.parser import parse
import seaborn as sns
import matplotlib.pyplot as plt

# Draw Plot
def plot_different_series(df, date_name, yaxis_field, title, ytitle):
    """
    Plot annual series for each customer, x axis: mont, y axis: yaxis_field
    :param df: pandas dataframe with the input data
    :param date_name: name of the field with the date
    :param yaxis_field: name of the field in the y-axis
    
    """
    import matplotlib.pyplot as plt
    df['year'] = [parse(d).year for d in df[date_name]]
    df['month'] = [parse(d).strftime('%b') for d in df[date_name]]
    years = df['year'].unique()
    df = df.sort_values(by=[date_name])

    mycolors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange', 'tab:brown', 'tab:grey', 'tab:pink', 'tab:olive', 'deeppink',
                'steelblue', 'firebrick', 'mediumseagreen']      
    plt.figure(figsize=(16,10), dpi= 58)

    for i, y in enumerate(years):
        plt.plot('month', yaxis_field, data=df.loc[df.year==y, :], color=mycolors[i], label=y)
        plt.text(df.loc[df.year==y, :].shape[0]-.9, df.loc[df.year==y, yaxis_field][-1:].values[0], y, fontsize=12, color=mycolors[i])

        # Decoration
        #plt.ylim(50,750)
        #plt.xlim(-0.3, 11)
        plt.ylabel(ytitle)
        plt.yticks(fontsize=12, alpha=.7)
        plt.xticks(fontsize=15, alpha=.7) # x tick size
        plt.title(title, fontsize=22)
        plt.grid(axis='y', alpha=.3)

        # Remove borders
        plt.gca().spines["top"].set_alpha(0.0)    
        plt.gca().spines["bottom"].set_alpha(0.5)
        plt.gca().spines["right"].set_alpha(0.0)    
        plt.gca().spines["left"].set_alpha(0.5)   
        plt.legend(loc='upper right', ncol=2, fontsize=12)

    plt.show()
    


def plot_density(df, density_field, date_name, title = ''):
    """
    Plot  density by year
    :param df: pandas dataframe with the input data
    :param density_field: which field use to get the density
    :param date_name: name of the field with the date
    :param title: title of the plot
    
    """
    
    df['year'] = [parse(d).year for d in df[date_name]]
    years = df['year'].unique()

    mycolors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange', 'tab:brown', 'tab:grey', 'tab:pink', 'tab:olive', 'deeppink',
                'steelblue', 'firebrick', 'mediumseagreen']      
    plt.figure(figsize=(16,10), dpi= 50)

    for i, y in enumerate(years):
        # Draw Plot
        sns.kdeplot(df.loc[df['year'] == y, density_field], shade=True, color=mycolors[i], label=y, alpha=.7)

    # Decoration
    plt.title(title, fontsize=22)
    plt.legend()
    plt.show()

def plot_box(df, classes, val, title):

    # Draw Plot
    plt.figure(figsize=(13,10), dpi= 50)
    sns.boxplot(x=classes, y=val, data=df, notch=False)

    # Add N Obs inside boxplot (optional)
    def add_n_obs(df,group_col,y):
        medians_dict = {grp[0]:grp[1][y].median() for grp in df.groupby(group_col)}
        xticklabels = [x.get_text() for x in plt.gca().get_xticklabels()]
        n_obs = df.groupby(group_col)[y].size().values
        for (x, xticklabel), n_ob in zip(enumerate(xticklabels), n_obs):
             plt.text(x, medians_dict[xticklabel]*1.01, "#obs : "+str(n_ob), horizontalalignment='center', fontdict={'size':14}, color='white')

    add_n_obs(df,group_col=classes,y=val)    

    # Decoration
    plt.title(title, fontsize=22)
    plt.ylim(10, 40)
    plt.show()

def plot_violin(df, classes, val, title):
    # Draw Plot
    plt.figure(figsize=(13,10), dpi= 40)
    sns.violinplot(x=classes, y=val, data=df, scale='width', inner='quartile')

    # Decoration
    plt.title(title, fontsize=22)
    plt.show()